import msgpack
from pymongo import MongoClient
from minio import Minio
from io import BytesIO
import sys
import os

base_directory = "./"
sys.path.insert(0, base_directory)
from orchestration.api.utils.uuid64 import Uuid64  # Import Uuid64 to format the image_uuid

# MongoDB connection
client = MongoClient("mongodb://192.168.3.1:32017/")
db = client['orchestration-job-db']  # Replace with your actual DB name

# Minio connection
minio_client = Minio(
    "192.168.3.5:9000",  
    access_key="v048BpXpWrsVIHUfdAix",  
    secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu", 
    secure=False  # Set to True if using HTTPS
)

# Function to count images in a dataset
def count_images(bucket_name, dataset):
    image_count = 0
    prefix = f"{dataset}/"
    
    # List all objects in the dataset's folder
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    
    for obj in objects:
        if obj.object_name.endswith(".jpg"):
            image_count += 1
    
    return image_count

# Function to count clip_vectors in msgpack files of a dataset
def count_clip_vectors(bucket_name, dataset):
    clip_vector_count = 0
    prefix = f"{dataset}/clip_vectors/"
    
    # List all objects in the clip_vectors folder
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    
    for obj in objects:
        if obj.object_name.endswith(".msgpack"):
            # Download and unpack msgpack file
            response = minio_client.get_object(bucket_name, obj.object_name)
            msgpack_data = msgpack.unpackb(response.read(), raw=False)
            response.close()
            response.release_conn()

            # Count clip_vectors in msgpack file
            for entry in msgpack_data:
                if "clip_vector" in entry:
                    clip_vector_count += 1

    return clip_vector_count

# Save datasets with fewer clip_vectors than images to a .txt file
def save_datasets_to_txt(file_path, datasets):
    with open(file_path, mode='w') as file:
        for dataset in datasets:
            file.write(f"{dataset}\n")
    print(f"Datasets saved to {file_path}")

# Process selected datasets and check if clip_vector count is less than image count
def process_selected_datasets():
    bucket_name = 'external'
    datasets = list_datasets(bucket_name)
    datasets_with_fewer_clip_vectors = []

    for dataset in datasets:
        try:
            image_count = count_images(bucket_name, dataset)
            clip_vector_count = count_clip_vectors(bucket_name, dataset)

            print(f"Dataset: {dataset} | Images: {image_count} | Clip Vectors: {clip_vector_count}")

            if clip_vector_count < image_count:
                print(f"Dataset {dataset} has fewer clip vectors than images!")
                datasets_with_fewer_clip_vectors.append(dataset)

        except Exception as e:
            print(f"Error processing dataset {dataset}: {e}")

    # Save the result to a .txt file
    save_datasets_to_txt("datasets_with_fewer_clip_vectors.txt", datasets_with_fewer_clip_vectors)

# List all top-level datasets in the external bucket
def list_datasets(bucket_name):
    datasets = set()  # Use a set to avoid duplicates
    objects = minio_client.list_objects(bucket_name, recursive=False)
    for obj in objects:
        top_level_folder = obj.object_name.split('/')[0]
        datasets.add(top_level_folder)
    return datasets

# Run the process
process_selected_datasets()
