import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
import msgpack

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
from utility.path import separate_bucket_and_file_path


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--minio-addr', help='Minio server address', default="192.168.3.5:9000")
    parser.add_argument('--minio-access-key', help='Minio access key')
    parser.add_argument('--minio-secret-key', help='Minio secret key')
    parser.add_argument('--dataset', type=str, help='Dataset to extract from')
    parser.add_argument('--min-quality-sigma', type=float, default=1, help='Minimum quality threshold')
    parser.add_argument('--min-classifier-score', type=float, default=0.7, help='Minimum classifier score threshold')
    parser.add_argument('--defect-threshold', type=float, default=0.6, help='Minimum defect threshold')
    parser.add_argument('--target-size', type=int, default=512, help='Target size of image extraction')
    parser.add_argument('--batch-size', type=int, default=10000, help='batch size for extraction')
    parser.add_argument('--file-batch-size', type=int, default=10000, help='Batch size for numpy file storage')

    return parser.parse_args()

def load_scoring_model(minio_client, rank_id, model_path, device):

    model_file_data =cmd.get_file_from_minio(minio_client, 'datasets', model_path)
    
    if model_file_data is None:
        print(f"No ranking model was found for rank {rank_id}.")
        return None

    scoring_model = ABRankingELMModel(1280, device=device)

    # Create a BytesIO object and write the downloaded content into it
    byte_buffer = io.BytesIO()
    for data in model_file_data.stream(amt=8192):
        byte_buffer.write(data)
    # Reset the buffer's position to the beginning
    byte_buffer.seek(0)

    scoring_model.load_safetensors(byte_buffer)

    print(f"model {model_path} loaded")

    return scoring_model

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
        self.topic_models= {}
        self.irrelevant_image_models= {}
        self.defect_models= {}
        self.clip = None
        self.vae = None

        # threads
        self.threads=[]

    def load_models(self):
        try:
            # load topic and defect models
            print("loading the classifier models")
            tags= request.http_get_tag_list()
            tag_names= [tag['tag_string'] for tag in tags]
            classifier_model= None
            target_tags=["game", "perspective"]

            for tag in tag_names:
                if tag.startswith("defect-"):
                    classifier_model= self.get_classifier_model(tag)
                    if classifier_model:
                        self.defect_models[tag]= classifier_model
                elif tag.startswith("irrelevant-"):
                    classifier_model= self.get_classifier_model(tag)
                    if classifier_model:
                        self.irrelevant_image_models[tag] = classifier_model
                elif any(tag.startswith(f"{prefix}-") for prefix in target_tags):
                    classifier_model= self.get_classifier_model(tag)
                    if classifier_model:
                        self.topic_models[tag]= classifier_model
                else:
                    continue
            
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
        

    def get_classifier_model(self, tag_name):
        input_path = f"environmental/models/classifiers/{tag_name}/"
        file_suffix = "elm-regression-clip-h-all_resolutions.pth"

        # Use the MinIO client's list_objects method directly with recursive=True
        model_files = [obj.object_name for obj in self.minio_client.list_objects('datasets', prefix=input_path, recursive=True) if obj.object_name.endswith(file_suffix)]
        
        if not model_files:
            print(f"No models found for tag: {tag_name}")
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
    
    def filter_external_images(self, images, clip_vectors):
        total_images = len(images)

        print("Filtering irrelevant images")
        for tag, model in tqdm(self.irrelevant_image_models.items()):
            with torch.no_grad():
                classifier_scores = model.classify(clip_vectors).squeeze()
            
            # Create a mask for filtering based on classifier scores
            mask = (classifier_scores < 3) & (classifier_scores < self.defect_threshold)

            # Apply the mask to filter the images 
            images = [image for image, keep in zip(images, mask) if keep]
            clip_vectors = clip_vectors[mask]

        print(f"{total_images - len(images)} images filtered as irrelevant")
        total_images = len(images)

        print("Filtering based on defects")
        for tag, model in tqdm(self.defect_models.items()):
            with torch.no_grad():
                classifier_scores = model.classify(clip_vectors).squeeze()
            
            # Create a mask for filtering based on classifier scores
            mask = (classifier_scores < 3) & (classifier_scores < self.defect_threshold)

            # Apply the mask to filter images and clip_vectors
            images = [image for image, keep in zip(images, mask) if keep]
            clip_vectors = clip_vectors[mask]

        print(f"{total_images - len(images)} images filtered as defective")
        total_images = len(images)

        print("Filtering for images with relevant content")
         # Initialize a mask to keep track of which images pass any topic model condition
        combined_mask = torch.zeros(len(images), dtype=torch.bool)

        # Also initialize a list to keep track of the highest classifier score for each image
        highest_scores = torch.full((len(images),), -float('inf'))

        for tag, model in tqdm(self.topic_models.items()):
            with torch.no_grad():
                classifier_scores = model.classify(clip_vectors).squeeze()

            # Create a mask for filtering based on classifier scores
            mask = (classifier_scores >= self.min_classifier_score) & (classifier_scores < 3)

            # Update the combined mask to keep images that satisfy any condition
            combined_mask |= mask

            # Update highest scores and track the best model for each image
            for idx, score in enumerate(classifier_scores):
                if score > highest_scores[idx]:
                    highest_scores[idx] = score
                    images[idx]['relevance_model'] = model

        # Apply the combined mask to filter images and clip_vectors
        images = [image for image, keep in zip(images, combined_mask) if keep]
        clip_vectors = clip_vectors[combined_mask]

        total_images = len(images)
        print(f"{total_images} images were selected after filtering")

        return images

    def upload_extracts(self, external_images: list, extracted_images: list):
        print("Uploading extracted images...........")
        extract_data=[]
        extraction_policy= "random_crop_resize"

        # filter the images based on
        index=0 
        for source_image, extract in tqdm(zip(external_images, extracted_images)):
            image = extract["image"]
            image_data = extract["image_data"]
            clip_vector= extract["clip_vector"]

            # calculate vae latent
            pixel_values = np.array(image).astype(np.float32) / 127.5 - 1  # Normalize
            pixel_values = np.transpose(pixel_values, [2, 0, 1])  # Correct channel order: [C, H, W]
            pixel_values = torch.from_numpy(pixel_values).unsqueeze(0).to(device=self.device)  # Add batch dimension

            with torch.no_grad():
                vae_latent = self.vae.encode(pixel_values).latents

            pixel_values.cpu()
            del pixel_values
            torch.cuda.empty_cache()

            data={
                "image_hash" : hashlib.md5(image_data.getvalue()).hexdigest(),
                "image_uuid": str(uuid.uuid4()),
                "image": image,
                "clip_vector": clip_vector,
                "vae_latent" : vae_latent,
                "source_image_hash": source_image["image_hash"],
                "source_image_uuid": source_image["uuid"],
                "extraction_policy": extraction_policy,
                "dataset": source_image["dataset"]
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
                # save batch file
                clip_vectors= self.clip_vectors.copy()
                vae_latents= self.vae_latents.copy()

                self.clip_vectors =[]
                self.vae_latents =[]

                thread = threading.Thread(target=save_latents_and_vectors, args=(self.minio_client, self.dataset, clip_vectors, vae_latents, self.image_hashes,))
                thread.start()
                self.threads.append(thread)
        
            index+=1
        
        # save any extra vectors to numpy files
        if len(self.clip_vectors) > 0:
            # save batch file
            clip_vectors= self.clip_vectors.copy()
            vae_latents= self.vae_latents.copy()

            self.clip_vectors =[]
            self.vae_latents =[]
            
            thread = threading.Thread(target=save_latents_and_vectors, args=(self.minio_client, self.dataset, clip_vectors, vae_latents, self.image_hashes,))
            thread.start()
            self.threads.append(thread)

        return extract_data

    def load_clip_vector(self, image_data):
        # get file path
        file_path= image_data["file_path"]

        # load clip vector
        bucket_name, input_file_path = separate_bucket_and_file_path(file_path)
        file_path = os.path.splitext(input_file_path)[0]

        clip_path = file_path + "_clip_kandinsky.msgpack"
        features_data = cmd.get_file_from_minio(self.minio_client, bucket_name, clip_path)
        clip_vector = msgpack.unpackb(features_data.data)["clip-feature-vector"]
        clip_vector = torch.tensor(clip_vector).squeeze()

        return image_data, clip_vector

    def extract_images(self):
        print("loading external dataset images..........")
        try:
            external_images= external_images_request.http_get_external_dataset_in_batches_without_extracts(dataset=self.dataset, batch_size=100000)
        except Exception as e:
            raise Exception(f"An error occured when querying the external image dataset: {e}.")
        
        total_images= len(external_images)
        print("total images loaded:", total_images)
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
           
            # filtering irrelevant images
            futures=[]
            with ThreadPoolExecutor(max_workers=10) as executor:
                for image in tqdm(images_batch):
                    futures.append(executor.submit(self.load_clip_vector, image))
            
            # Collect results as they complete
            clip_vectors=[]
            images_batch=[]
            print("Loading clip vectors for the image batch")
            for future in tqdm(as_completed(futures)):
                try:
                    image_data, clip_vector = future.result()
                    clip_vectors.append(clip_vector)
                    images_batch.append(image_data)
                except Exception as e:
                    print(f"Failed to load clip vectors for an image: {e}")
                
            # filter irrelevant images
            clip_vectors= torch.stack(clip_vectors)
            filtered_batch= self.filter_external_images(images_batch, clip_vectors)

            # extracting the 512*512 image patches
            extracts= extract_square_images(self.minio_client, self.clip, filtered_batch, self.target_size)

            # upload the extracts to minio and mongoDB
            extract_data= self.upload_extracts(external_images= filtered_batch,
                                               extracted_images= extracts)
            
            processed_images+= len(extract_data)
            print(f"{len(extract_data)} images extracted from {self.batch_size} images")
            print(f"total extracted images: {processed_images}/{total_images}")

        # check if all upload threads are completed
        for thread in self.threads:
            thread.join()

def main():
    args= parse_args()

    if args.dataset == "all_games":
        games= external_images_request.http_get_video_game_list()

        print(f"list of games: {games}")

        for game in games:
            dataset = game["title"]

            external_images_request.http_add_dataset(dataset_name=dataset, bucket_id=1)

            # initialize image extraction pipeline
            pipeline= ImageExtractionPipeline(minio_access_key=args.minio_access_key,
                                                minio_secret_key=args.minio_secret_key,
                                                dataset=dataset,
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

    else:
        external_images_request.http_add_dataset(dataset_name=args.dataset, bucket_id=1)

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
         

