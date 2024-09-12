import msgpack
from pymongo import MongoClient
from minio import Minio
from io import BytesIO
import csv
import sys
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

# Collection for storing orphaned image hashes
orphaned_hashes = []

# Function to fetch and format image_uuid based on image_hash from the respective collection
def get_image_uuid(image_hash, collection):
    document = collection.find_one({"image_hash": image_hash}, {"image_uuid": 1})
    if document:
        # Format the image_uuid using Uuid64
        formatted_image_uuid = Uuid64.from_mongo_value(document.get('image_uuid')).to_formatted_str()
        print(f"Found for image_hash {image_hash}: image_uuid (formatted): {formatted_image_uuid}")
        return formatted_image_uuid
    print(f"No image_uuid found for image_hash: {image_hash}")
    return None

# Function to determine the correct collection based on the bucket name
def get_collection(bucket_name):
    if bucket_name == 'extract':
        print(f"Using collection: extracts for bucket: {bucket_name}")
        return db['extracts']  # Collection for 'extract' bucket
    elif bucket_name == 'external':
        print(f"Using collection: external_images for bucket: {bucket_name}")
        return db['external_images']  # Collection for 'external' bucket
    else:
        raise ValueError(f"Unknown bucket name: {bucket_name}")

# Function to update msgpack data with formatted image_uuid, and keep the uuid from the msgpack itself
def update_msgpack_data(data, bucket_collection):
    print(f"Updating msgpack data...")
    for entry in data:
        uuid_value = entry.get("uuid")
        image_hash = entry.get("image_hash")
        clip_vector = entry.get("clip_vector")
        
        if image_hash and uuid_value:
            print(f"Processing image_hash: {image_hash} with existing uuid: {uuid_value}")
            image_uuid = get_image_uuid(image_hash, bucket_collection)  # Fetch and format image_uuid

            if image_uuid:
                # Rebuild the entry, keeping uuid from the msgpack and updating image_uuid with formatted value
                reordered_entry = {
                    "uuid": uuid_value,
                    "image_uuid": image_uuid,  # Using the formatted image_uuid
                    "image_hash": image_hash,
                    "clip_vector": clip_vector
                }
                # Update the entry with the reordered dictionary
                entry.clear()  # Clear the existing dictionary
                entry.update(reordered_entry)  # Update with the new order
                print(f"Updated and reordered entry for image_hash {image_hash}")
            
            # If no image_uuid is found, log as orphaned
            if not image_uuid:
                print(f"Marking image_hash: {image_hash} as orphaned")
                orphaned_hashes.append(image_hash)
    return data

# Save orphaned image_hashes to CSV
def save_orphaned_hashes_to_csv(file_path):
    print(f"Saving orphaned image_hashes to CSV at: {file_path}")
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["orphaned_image_hash"])  # Write CSV header
        for image_hash in orphaned_hashes:
            writer.writerow([image_hash])
    print(f"Orphaned image_hashes saved.")

# Process msgpack files in a specific dataset
def process_msgpack(bucket_name, file_path):
    print(f"Processing msgpack file: {file_path} in bucket: {bucket_name}")
    
    # Get the correct collection for the bucket
    bucket_collection = get_collection(bucket_name)

    # Download the msgpack file from Minio
    print(f"Downloading msgpack file: {file_path} from bucket: {bucket_name}")
    response = minio_client.get_object(bucket_name, file_path)
    msgpack_data = msgpack.unpackb(response.read(), raw=False)
    response.close()
    response.release_conn()

    # Update the msgpack data with formatted image_uuid, keeping uuid from the msgpack itself
    updated_data = update_msgpack_data(msgpack_data, bucket_collection)

    # Convert updated data back to msgpack format
    updated_msgpack = BytesIO()

    # Packing the updated data
    print("Packing updated data back to msgpack format")
    msgpack.pack(updated_data, updated_msgpack, use_bin_type=True)  # Corrected: Pass the stream (updated_msgpack)

    # Make sure to reset the BytesIO buffer to the start before uploading
    updated_msgpack.seek(0)

    print(f"Uploading updated msgpack file: {file_path} to bucket: {bucket_name}")
    minio_client.put_object(bucket_name, file_path, data=updated_msgpack, length=updated_msgpack.getbuffer().nbytes)
    print(f"Uploaded updated msgpack file: {file_path} to bucket: {bucket_name}")

    # Save orphaned image_hashes to CSV
    save_orphaned_hashes_to_csv("orphaned_image_hashes.csv")

# List all top-level datasets in the external bucket
def list_datasets(bucket_name):
    datasets = set()  # Use a set to avoid duplicates
    objects = minio_client.list_objects(bucket_name, recursive=False)
    for obj in objects:
        top_level_folder = obj.object_name.split('/')[0]
        datasets.add(top_level_folder)
    print(datasets)    
    return datasets

# Process selected datasets
def process_selected_datasets():
    bucket_name = 'external'
    datasets = list_datasets(bucket_name)
    for dataset in datasets:
        # Build the path to the specific clip_vectors msgpack file
        file_path = f"{dataset}/clip_vectors/0001_clip_data.msgpack"
        try:
            print(f"Processing dataset: {file_path}")
            process_msgpack(bucket_name, file_path)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

# Example usage: Process specific datasets from external bucket
process_selected_datasets()
