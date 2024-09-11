import msgpack
from pymongo import MongoClient
from minio import Minio
from io import BytesIO
import csv

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

# Function to fetch image_uuid based on image_hash from all-images collection
def get_image_uuid(image_hash):
    # Use the all-images collection to find the image_uuid
    collection = db['all-images']
    document = collection.find_one({"image_hash": image_hash}, {"uuid": 1})
    if document:
        return document["uuid"]
    return None

# Function to determine the correct collection based on the bucket name
def get_collection(bucket_name):
    if bucket_name == 'extract':
        return db['extracts']  # Collection for 'extract' bucket
    elif bucket_name == 'external':
        return db['external_images']  # Collection for 'external' bucket
    else:
        raise ValueError(f"Unknown bucket name: {bucket_name}")

# Function to update msgpack data
def update_msgpack_data(data, bucket_name):
    for entry in data:
        image_hash = entry.get("image_hash")
        if image_hash:
            image_uuid = get_image_uuid(image_hash)  # Fetch from all-images collection
            if image_uuid:
                # Insert image_uuid after uuid in the dictionary
                if "uuid" in entry:
                    uuid_value = entry.pop("uuid")  # Remove and save the uuid value
                    entry["uuid"] = uuid_value  # Re-insert the uuid at the beginning
                    entry["image_uuid"] = image_uuid  # Add image_uuid after uuid
            else:
                # If no image_uuid found, it's an orphaned hash, save it to orphaned list
                orphaned_hashes.append(image_hash)
    return data

# Save orphaned image_hashes to CSV
def save_orphaned_hashes_to_csv(file_path):
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["orphaned_image_hash"])  # Write CSV header
        for image_hash in orphaned_hashes:
            writer.writerow([image_hash])

# Process msgpack files in a bucket
def process_msgpack(bucket_name, file_path):
    # Get the correct collection for the bucket
    collection = get_collection(bucket_name)

    # Download the msgpack file from Minio
    response = minio_client.get_object(bucket_name, file_path)
    msgpack_data = msgpack.unpackb(response.read(), raw=False)
    response.close()
    response.release_conn()

    # Update the msgpack data
    updated_data = update_msgpack_data(msgpack_data, bucket_name)

    # Convert updated data back to msgpack format
    updated_msgpack = BytesIO()
    msgpack.packb(updated_data, updated_msgpack, use_bin_type=True)

    # Upload the updated msgpack back to Minio
    updated_msgpack.seek(0)
    minio_client.put_object(bucket_name, file_path, data=updated_msgpack, length=updated_msgpack.getbuffer().nbytes)

    # Save orphaned image_hashes to CSV
    save_orphaned_hashes_to_csv("orphaned_image_hashes.csv")

# Example usage
bucket_name = 'external'  # or 'extract'
file_path = 'trine/clip_vectors/0001_clip_data.msgpack'  # Replace with the actual path
process_msgpack(bucket_name, file_path)
