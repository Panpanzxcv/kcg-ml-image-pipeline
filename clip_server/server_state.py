from io import BytesIO
import sys
import msgpack
import time
import numpy as np
import torch
from PIL import Image

base_directory = "./"
sys.path.insert(0, base_directory)

from kandinsky.models.clip_text_encoder.clip_text_encoder import KandinskyCLIPTextEmbedder
from utility.clip.clip import ClipModel
from kandinsky.models.clip_image_encoder.clip_image_encoder import KandinskyCLIPImageEncoder
from utility.minio.cmd import get_file_from_minio, is_object_exists
from utility.path import separate_bucket_and_file_path
from clip_cache import ClipCache
from clip_constants import CLIP_CACHE_DIRECTORY
from utility.http.request import http_get_list_completed_jobs
from utility.http.external_images_request import http_get_external_image_list, http_get_extract_image_list

class Phrase:
    def __init__(self, id, phrase):
        self.id = id
        self.phrase = phrase

class ClipVector:
    def __init__(self, phrase, clip_vector):
        self.phrase = phrase
        self.clip_vector = clip_vector

class ClipServer:
    def __init__(self, device, minio_client):
        self.minio_client = minio_client
        self.id_counter = 0
        self.phrase_dictionary = {}
        self.clip_vector_dictionary = {}
        self.image_clip_vector_cache = {}
        self.clip_model = KandinskyCLIPTextEmbedder(device=device)
        self.kandinsky_clip_model= KandinskyCLIPImageEncoder(device=device)
        self.device = device
        self.clip_cache = ClipCache(device, minio_client, CLIP_CACHE_DIRECTORY)

    def load_clip_model(self):
        self.clip_model.load_submodels()
        self.kandinsky_clip_model.load_submodels()

    def generate_id(self):
        new_id = self.id_counter
        self.id_counter = self.id_counter + 1

        return new_id

    def compute_kandinsky_image_clip_vector(self, image_path):
        bucket_name, file_path = separate_bucket_and_file_path(image_path)
        try:
            response = self.minio_client.get_object(bucket_name, file_path)
            image_data = BytesIO(response.data)
            img = Image.open(image_data)
            img = img.convert("RGB")
        except Exception as e:
            raise e
        finally:
            response.close()
            response.release_conn()
        
        # get feature
        clip_feature_vector = self.kandinsky_clip_model.get_image_features(img)

        # put to cpu
        clip_feature_vector = clip_feature_vector.cpu().detach()

        # convert to np array
        clip_feature_vector_np_arr = np.array(clip_feature_vector, dtype=np.float32)

        # convert to normal list
        clip_feature_vector_arr = clip_feature_vector_np_arr.tolist()

        return clip_feature_vector_arr

    def add_phrase(self, phrase):
        new_id = self.generate_id()
        clip_vector = self.compute_clip_vector(phrase)

        new_phrase = Phrase(new_id, phrase)
        new_clip_vector = ClipVector(phrase, clip_vector)

        self.phrase_dictionary[new_id] = new_phrase
        self.clip_vector_dictionary[phrase] = new_clip_vector

        return new_phrase

    def get_clip_vector(self, phrase):
        if phrase in self.clip_vector_dictionary:
            return self.clip_vector_dictionary[phrase]

        return None

    def get_image_clip_vector(self, bucket, image_path):
        return self.clip_cache.get_clip_vector(bucket, image_path)

    def get_phrase_list(self, offset, limit):
        result = []
        count = 0
        for key, value in self.phrase_dictionary.items():
            if count >= offset:
                if count < offset + limit:
                    result.append(value)
                else:
                    break
            count += 1
        return result


    def get_image_clip_from_minio(self, image_path, bucket_name):

        # if its in the cache return from cache
        if image_path in self.image_clip_vector_cache:
            clip_vector = self.image_clip_vector_cache[image_path]
            return clip_vector

        # Removes the last 4 characters from the path
        # image.jpg => image
        base_path = image_path.rstrip(image_path[-4:])

        # finds the clip file associated with the image
        image_clip_vector_path = f'{base_path}_clip_kandinsky.msgpack'

        print(f'image clip vector path : {image_clip_vector_path}')
        # get the clip.msgpack from minio
        file_exists = is_object_exists(self.minio_client, bucket_name, image_clip_vector_path)

        if not file_exists:
            print(f'{image_clip_vector_path} does not exist')
            return None

        clip_vector_data_msgpack = get_file_from_minio(self.minio_client, bucket_name, image_clip_vector_path)

        if clip_vector_data_msgpack is None:
            print(f'image not found {image_path}')
            return None

        # read file_data_into memory
        clip_vector_data_msgpack_memory = clip_vector_data_msgpack.read()

        try:
            # uncompress the msgpack data
            clip_vector = msgpack.unpackb(clip_vector_data_msgpack_memory)
            clip_vector = clip_vector["clip-feature-vector"]
            # add to chache
            self.image_clip_vector_cache[image_path] = clip_vector

            return clip_vector
        except Exception as e:
            print('Exception details : ', e)

        return None


    def compute_cosine_match_value(self, phrase, bucket, image_path):
        print('computing cosine match value for ', phrase, ' and ', image_path)

        phrase_cip_vector_struct = self.get_clip_vector(phrase)
        # the score is zero if we cant find the phrase clip vector
        if phrase_cip_vector_struct is None:
            print(f'phrase {phrase} not found ')
            return 0

        phrase_clip_vector_numpy = phrase_cip_vector_struct.clip_vector

        image_clip_vector_numpy = self.get_image_clip_vector(bucket, image_path)

        # the score is zero if we cant find the image clip vector
        if image_clip_vector_numpy is None:
            print(f'image clip {image_path} not found')
            return 0

        # convert numpy array to tensors
        phrase_clip_vector = torch.tensor(phrase_clip_vector_numpy, dtype=torch.float32, device=self.device)
        image_clip_vector = torch.tensor(image_clip_vector_numpy, dtype=torch.float32, device=self.device)

        #check the vector size
        assert phrase_clip_vector.size() == (1, 1280), f"Expected size (1, 1280), but got {phrase_clip_vector.size()}"
        assert image_clip_vector.size() == (1, 1280), f"Expected size (1, 1280), but got {image_clip_vector.size()}"

        # removing the extra dimension
        # from shape (1, 768) => (768)
        phrase_clip_vector = phrase_clip_vector.squeeze(0)
        image_clip_vector = image_clip_vector.squeeze(0)

        # Normalizing the tensor
        normalized_phrase_clip_vector = torch.nn.functional.normalize(phrase_clip_vector.unsqueeze(0), p=2, dim=1)
        normalized_image_clip_vector = torch.nn.functional.normalize(image_clip_vector.unsqueeze(0), p=2, dim=1)

        # removing the extra dimension
        # from shape (1, 768) => (768)
        normalized_phrase_clip_vector = normalized_phrase_clip_vector.squeeze(0)
        normalized_image_clip_vector = normalized_image_clip_vector.squeeze(0)

        # cosine similarity
        similarity = torch.dot(normalized_phrase_clip_vector, normalized_image_clip_vector)

        # cleanup
        del phrase_clip_vector
        del image_clip_vector
        del normalized_phrase_clip_vector
        del normalized_image_clip_vector

        return similarity.item()

    def compute_cosine_match_value_list(self, phrase, bucket, image_path_list):

        num_images = len(image_path_list)

        print(f'computing cosine match value for {num_images} images')
        # Record the start time
        start_time = time.time()

        # vector full of zeroes of size=num_images
        cosine_match_list = [0] * num_images

        phrase_cip_vector_struct = self.get_clip_vector(phrase)
        # the score is zero if we cant find the phrase clip vector
        if phrase_cip_vector_struct is None:
            print(f'phrase {phrase} not found ')
            return cosine_match_list

        phrase_clip_vector_numpy = phrase_cip_vector_struct.clip_vector

        # convert numpy array to tensors
        phrase_clip_vector = torch.tensor(phrase_clip_vector_numpy, dtype=torch.float32, device=self.device)

         #check the vector size
        assert phrase_clip_vector.size() == (1, 1280), f"Expected size (1, 1280), but got {phrase_clip_vector.size()}"

        # Normalizing the tensor
        normalized_phrase_clip_vector = torch.nn.functional.normalize(phrase_clip_vector, p=2, dim=1)

        # removing the extra dimension
        # from shape (1, 768) => (768)
        normalized_phrase_clip_vector = normalized_phrase_clip_vector.squeeze(0)

        # for each batch do
        for image_index in range(0, num_images):
            image_path = image_path_list[image_index]
            image_clip_vector = self.get_image_clip_vector(bucket, image_path)
            # if the clip_vector was not found
            # or couldn't load for some network reason
            # we must provide an empty vector as replacement
            if image_clip_vector is None:
                # this syntax is weird but its just list full of zeros
                cosine_match_list[image_index] = 0
                continue

            # now that we have the clip vectors we need to construct our tensors
            image_clip_vector = torch.tensor(image_clip_vector, dtype=torch.float32, device=self.device)

            #check the vector size
            assert image_clip_vector.size() == (1, 1280), f"Expected size (1, 1280), but got {image_clip_vector.size()}"

            normalized_image_clip_vector = torch.nn.functional.normalize(image_clip_vector, p=2, dim=1)
            # removing the extra dimension
            # from shape (1, 768) => (768)
            normalized_image_clip_vector = normalized_image_clip_vector.squeeze(0)

            # cosine similarity
            similarity = torch.dot(normalized_phrase_clip_vector, normalized_image_clip_vector)
            similarity_value = similarity.item()
            cosine_match_list[image_index] = similarity_value

            # cleanup
            del image_clip_vector
            del normalized_image_clip_vector
            del similarity
            # After your GPU-related operations, clean up the GPU memory
            torch.cuda.empty_cache()

        del phrase_clip_vector
        del normalized_phrase_clip_vector
        # After your GPU-related operations, clean up the GPU memory
        torch.cuda.empty_cache()

        # Record the end time
        end_time = time.time()

        # Calculate the elapsed time
        elapsed_time = end_time - start_time

        print(f"Function execution time: {elapsed_time:.4f} seconds")

        return cosine_match_list

    def compute_clip_vector(self, text):
        _, clip_vector_gpu, _ = self.clip_model.compute_embeddings(text)
        clip_vector_cpu = clip_vector_gpu.cpu()

        del clip_vector_gpu

        clip_vector = clip_vector_cpu.tolist()
        return clip_vector

    def download_all_clip_vectors(self, bucket):

        print('Starting to download all clip vectors')

        print('Getting list of completed jobs')
        
        if bucket=="datasets":
            completed_jobs = http_get_list_completed_jobs()
        elif bucket=="external":
            completed_jobs = http_get_external_image_list()
        elif bucket=="extracts":
            completed_jobs = http_get_extract_image_list()
        else:
            print(f"Bucket name {bucket} not recognized")
            return None
        
        print('Finished getting list of completed jobs')

        if completed_jobs is None:
            print('Could not get list of completed jobs')
            return None

        num_jobs = len(completed_jobs)
        job_index = 0
        for job in completed_jobs:
            print(f'processing job {job_index} our of {num_jobs}')
            job_index = job_index + 1
            if bucket=="datasets":
                input_dict = job['task_input_dict']

                # Jobs must have input dictionary
                if input_dict is None:
                    continue

                # Jobs must have target dataset
                if 'dataset' not in input_dict:
                    continue

                # Jobs must have output image path
                if 'file_path' not in input_dict:
                    continue

                dataset = input_dict['dataset']
                file_path = input_dict['file_path']

                image_path = f'{dataset}/{file_path}'

            elif bucket in ['external', 'extracts']:
                _ , image_path = separate_bucket_and_file_path(job['file_path'])

            # this will download the clip vector from minio
            # and will also add it to clip cache
            self.clip_cache.get_clip_vector(bucket, image_path)



