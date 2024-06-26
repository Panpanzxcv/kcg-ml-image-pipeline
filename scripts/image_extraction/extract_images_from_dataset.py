import argparse
import hashlib
import io
import math
import os
import sys
import threading
import uuid
import numpy as np
import torch
from diffusers import VQModel
from tqdm import tqdm

base_dir = "./"
sys.path.insert(0, base_dir)
sys.path.insert(0, os.getcwd())

from training_worker.ab_ranking.model.ab_ranking_elm_v1 import ABRankingELMModel
from utility.minio import cmd
from utility.http import request
from utility.http import external_images_request
from kandinsky.models.clip_image_encoder.clip_image_encoder import KandinskyCLIPImageEncoder
from scripts.image_extraction.utils import extract_square_images, save_latents_and_vectors, upload_extract_data
from training_worker.classifiers.models.elm_regression import ELMRegression
from kandinsky.model_paths import DECODER_MODEL_PATH


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--minio-addr', help='Minio server address', default="192.168.3.5:9000")
    parser.add_argument('--minio-access-key', help='Minio access key')
    parser.add_argument('--minio-secret-key', help='Minio secret key')
    parser.add_argument('--dataset', type=str, help='Dataset to extract from')
    parser.add_argument('--min-quality-sigma', type=float, default=1, help='Minimum quality threshold')
    parser.add_argument('--min-classifier-score', type=float, default=0.7, help='Minimum classifier score threshold')
    parser.add_argument('--defect-threshold', type=float, default=0.7, help='Minimum defect threshold')
    parser.add_argument('--target-size', type=int, default=512, help='Target size of image extraction')
    parser.add_argument('--batch-size', type=int, default=10000, help='batch size for extraction')
    parser.add_argument('--file-batch-size', type=int, default=10000, help='Batch size for numpy file storage')

    return parser.parse_args()

class ImageExtractionPipeline:

    def __init__(self,
                 minio_access_key,
                 minio_secret_key,
                 dataset: str,
                 min_quality_sigma: float = 1,
                 min_classifier_score: float = 0.6,
                 defect_threshold: float = 0.7,
                 target_size: int = 512,
                 batch_size: int = 10000,
                 file_batch_size= 10000):
        
        # get minio client
        self.minio_client = cmd.get_minio_client(minio_access_key=minio_access_key,
                                                minio_secret_key=minio_secret_key)

        # set parameters
        self.dataset= dataset
        self.min_quality_sigma= min_quality_sigma
        self.min_classifier_score= min_classifier_score
        self.defect_threshold= defect_threshold
        self.target_size= target_size
        self.batch_size= batch_size
        self.file_batch_size= file_batch_size
        self.clip_vectors=[]
        self.vae_latents=[]
        self.image_hashes= []

        # get device
        if torch.cuda.is_available():
            device = 'cuda'
        else:
            device = 'cpu'
        self.device = torch.device(device)

        # models
        self.quality_models= {}
        self.topic_models= {}
        self.defect_models= {}
        self.clip = None
        self.vae = None

        # threads
        self.threads=[]

    def load_models(self):

        try:
            print("loading the ranking models")
            # load ranking models
            dataset_list= request.http_get_dataset_names()

            for dataset in dataset_list:
                scoring_model= self.load_scoring_model(dataset)

                if scoring_model is not None:
                        self.quality_models[dataset]= scoring_model
            
            # load topic and defect models
            print("loading the classifier models")
            tags= request.http_get_tag_list()
            tag_names= [tag['tag_string'] for tag in tags]
            tag_types= ['defect', 'topic', 'perspective', 'style', 'concept']

            for tag in tag_names:
                if any(tag_type in tag for tag_type in tag_types):
                    classifier_model= self.get_classifier_model(tag)
                    if classifier_model:
                        if "defect" in tag:
                            self.defect_models[tag]= classifier_model
                        else:
                            self.topic_models[tag]= classifier_model
            
            print("Loading the image encoder")
            # load clip image encoder
            self.clip = KandinskyCLIPImageEncoder(device= self.device)
            self.clip.load_submodels()

            print("Loading the vae encoder")
            self.vae = VQModel.from_pretrained(
                DECODER_MODEL_PATH, subfolder="movq",
                local_files_only=True,
            ).eval().to(device=self.device)

        except Exception as e:
            raise Exception(f"An error occured while loading the models: {e}.")
        

    # load elm scoring models
    def load_scoring_model(self, dataset):
        input_path=f"{dataset}/models/ranking/"
        scoring_model = ABRankingELMModel(1280, device=self.device)
        file_name=f"score-elm-v1-clip-h.safetensors"

        model_files=cmd.get_list_of_objects_with_prefix(self.minio_client, 'datasets', input_path)
        most_recent_model = None

        for model_file in model_files:
            if model_file.endswith(file_name):
                most_recent_model = model_file

        if most_recent_model:
            model_file_data =cmd.get_file_from_minio(self.minio_client, 'datasets', most_recent_model)
        else:
            print("No .safetensors files found in the list.")
            return None

        print(most_recent_model)

        # Create a BytesIO object and write the downloaded content into it
        byte_buffer = io.BytesIO()
        for data in model_file_data.stream(amt=8192):
            byte_buffer.write(data)
        # Reset the buffer's position to the beginning
        byte_buffer.seek(0)

        scoring_model.load_safetensors(byte_buffer)
        scoring_model.model=scoring_model.model.to(torch.device(self.device))

        return scoring_model 

    def get_classifier_model(self, tag_name):
        input_path = f"environmental/models/classifiers/{tag_name}/"
        file_suffix = "elm-regression-clip-h.safetensors"

        # Use the MinIO client's list_objects method directly with recursive=True
        model_files = [obj.object_name for obj in self.minio_client.list_objects('datasets', prefix=input_path, recursive=True) if obj.object_name.endswith(file_suffix)]
        
        if not model_files:
            print(f"No .safetensors models found for tag: {tag_name}")
            return None

        # Assuming there's only one model per tag or choosing the first one
        model_files.sort(reverse=True)
        model_file = model_files[0]
        print(f"Loading model: {model_file}")
        
        model_data = self.minio_client.get_object('datasets', model_file)
        
        clip_model = ELMRegression(device=self.device)
        
        # Create a BytesIO object from the model data
        byte_buffer = io.BytesIO(model_data.data)
        clip_model.load_safetensors(byte_buffer)

        print(f"Model loaded for tag: {tag_name}")
        
        return clip_model
    
    def is_filtered(self, clip_vector):
        above_quality_threshold= False
        above_classifier_threshold= False
        # Check quality score
        for dataset, model in self.quality_models.items():
            with torch.no_grad():
                clip_score = model.predict_clip(clip_vector).item()
                score_mean= float(model.mean)
                score_std= float(model.standard_deviation)
                sigma_score = (clip_score - score_mean) / score_std
            
            if sigma_score > self.min_quality_sigma:
                above_quality_threshold =True
                break

        # check classifier scores
        if not above_quality_threshold:
            for tag, model in self.topic_models.items():
                with torch.no_grad():
                    classifier_score = model.classify(clip_vector).item()
                if classifier_score > self.min_classifier_score:
                    above_classifier_threshold= True
                    break
        
        if above_classifier_threshold or above_quality_threshold:
            # check if the image has any defects
            for tag, model in self.defect_models.items():
                with torch.no_grad():
                    classifier_score = model.classify(clip_vector).item()
                if classifier_score >= self.defect_threshold:
                    return True
        else:
            return True
        
        return False

    def filter_extracts(self, external_images: list, extracted_images: list):
        print("Filtering extracted images...........")
        extract_data=[]
        extraction_policy= "random_crop_resize"

        # filter the images based on
        index=0 
        for extract in tqdm(extracted_images):
            image = extract["image"]
            image_data = extract["image_data"]

            # get the clip vector from the image
            with torch.no_grad():
                clip_vector= self.clip.get_image_features(image).to(dtype=torch.float32)
            # filter the image if it's not useful
            if not self.is_filtered(clip_vector):
                # calculate vae latent
                pixel_values = np.array(image).astype(np.float32) / 127.5 - 1  # Normalize
                pixel_values = np.transpose(pixel_values, [2, 0, 1])  # Correct channel order: [C, H, W]
                pixel_values = torch.from_numpy(pixel_values).unsqueeze(0).to(device=self.device)  # Add batch dimension

                with torch.no_grad():
                    vae_latent = self.vae.encode(pixel_values).latents

                pixel_values.cpu()
                del pixel_values
                torch.cuda.empty_cache()

                # store data
                source_image_data= external_images[index]
                
                data={
                    "image_hash" : hashlib.md5(image_data.getvalue()).hexdigest(),
                    "image_uuid": str(uuid.uuid4()),
                    "image": image,
                    "clip_vector": clip_vector,
                    "vae_latent" : vae_latent,
                    "source_image_hash": source_image_data["image_hash"],
                    "source_image_uuid": source_image_data["uuid"],
                    "extraction_policy": extraction_policy,
                    "dataset": source_image_data["dataset"]
                }

                extract_data.append(data)

                # spawn upload data thread
                thread = threading.Thread(target=upload_extract_data, args=(self.minio_client, data,))
                thread.start()
                self.threads.append(thread)

                self.clip_vectors.append(clip_vector)
                self.vae_latents.append(vae_latent)
                self.image_hashes.append(data["image_hash"])
                
                # check if batch size was reached
                if len(self.clip_vectors) >= self.file_batch_size:
                    # save numpy files
                    thread = threading.Thread(target=save_latents_and_vectors, args=(self.minio_client, self.dataset, self.clip_vectors, self.vae_latents, self.image_hashes,))
                    thread.start()
                    self.threads.append(thread)
            
            index+=1
        
        # save any extra vectors to numpy files
        if len(self.clip_vectors) > 0:
            # save numpy files
            thread = threading.Thread(target=save_latents_and_vectors, args=(self.minio_client, self.dataset, self.clip_vectors, self.vae_latents, self.image_hashes,))
            thread.start()
            self.threads.append(thread)

        return extract_data

    def extract_images(self):
        print("loading external dataset images..........")
        try:
            external_images= external_images_request.http_get_external_image_list_without_extracts(dataset=self.dataset)
        except Exception as e:
            raise Exception(f"An error occured when querying the external image dataset: {e}.")
        
        total_images= len(external_images)
        print("total images:", total_images)
        processed_images= 0
        print("Extracting images.......")
        num_batches= math.ceil(total_images / self.batch_size)
        for batch_iter in range(0, num_batches):
            print(f"processing batch {batch_iter}")
            # getting start and end index for the batch
            start_index= batch_iter * self.batch_size
            end_index = min((batch_iter + 1) * self.batch_size, total_images)

            # getting the batch
            images_batch= external_images[start_index:end_index]

            # extracting the 512*512 image patches
            extracts= extract_square_images(self.minio_client, images_batch, self.target_size)

            # filter the extracts by quality
            extract_data= self.filter_extracts(external_images= images_batch,
                                               extracted_images= extracts)
            
            processed_images+= len(extract_data)
            print(f"{len(extract_data)} images filtered from {self.batch_size} images")
            print(f"total extracted images: {processed_images}")

        # check if all upload threads are completed
        for thread in self.threads:
            thread.join()

def main():
    args= parse_args()

    # initialize image extraction pipeline
    pipeline= ImageExtractionPipeline(minio_access_key=args.minio_access_key,
                                        minio_secret_key=args.minio_secret_key,
                                        dataset=args.dataset,
                                        min_quality_sigma= args.min_quality_sigma,
                                        min_classifier_score= args.min_classifier_score,
                                        defect_threshold= args.defect_threshold,
                                        target_size= args.target_size,
                                        batch_size= args.batch_size,
                                        file_batch_size= args.file_batch_size) 
    # load all necessary models
    pipeline.load_models()

    # run image extraction
    pipeline.extract_images()

if __name__ == "__main__":
    main()
         
