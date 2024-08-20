import time
import datetime
from pymongo import MongoClient, UpdateOne

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"
DATABASE_NAME = "orchestration-job-db"
COMPLETED_JOBS_COLLECTION = "completed-jobs"
EXTRACTS_COLLECTION = "extracts"
EXTERNAL_IMAGES_COLLECTION = "external_images"
ALL_IMAGES_COLLECTION = "all-images"

# Connect to MongoDB
print("Connecting to MongoDB...")
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
completed_jobs_collection = db[COMPLETED_JOBS_COLLECTION]
extracts_collection = db[EXTRACTS_COLLECTION]
external_images_collection = db[EXTERNAL_IMAGES_COLLECTION]
all_images_collection = db[ALL_IMAGES_COLLECTION]

# Determine the target collection for the job based on the hash
def determine_target_collection(image_hash):
    if completed_jobs_collection.find_one({"task_output_file_dict.output_file_hash": image_hash}) is not None:
        return completed_jobs_collection, "task_output_file_dict.output_file_hash"
    elif extracts_collection.find_one({"image_hash": image_hash}) is not None:
        return extracts_collection, "image_hash"
    elif external_images_collection.find_one({"image_hash": image_hash}) is not None:
        return external_images_collection, "image_hash"
    else:
        return None, None

# Check if the job should be skipped
def should_skip_job(job, target_collection):
    if "image_uuid" in job:
        return True
    if target_collection == completed_jobs_collection and "task_type" in job and "clip" in job["task_type"]:
        return True
    return False

# Process a single job
def process_job(job):
    image_hash = job.get("image_hash")
    if not image_hash:
        print("Skipping job due to missing image_hash")
        return None  # Skip if required fields are not available

    uuid = job.get("uuid")
    if not uuid:
        print("Skipping job due to missing uuid")
        return None  # Skip if uuid is not available

    print(f"Determining target collection for image_hash: {image_hash}")
    target_collection, field_path = determine_target_collection(image_hash)
    if target_collection is None:
        print(f"Skipping job due to undefined target collection for image_hash: {image_hash}")
        return None

    print(f"Checking if job should be skipped for image_hash: {image_hash} with uuid: {uuid}")
    if should_skip_job(job, target_collection):
        print(f"Skipping job with uuid {uuid} due to task_type containing 'clip' or existing image_uuid")
        return None

    print(f"Job with uuid {uuid} will be processed and migrated to image_uuid")
    job["image_uuid"] = uuid  # Migrate the existing uuid as image_uuid

    return job, target_collection, field_path


# Process all documents in all_images_collection
print("Processing all documents in all_images_collection...")

try:
    batch_size = 1000  # Adjust batch size as needed
    bulk_operations = {}

    cursor = all_images_collection.find(no_cursor_timeout=True).batch_size(batch_size)
    for job in cursor:
        image_hash = job.get("image_hash")
        if image_hash:
            print(f"Found job with image_hash: {image_hash} -> {job}")

            result = process_job(job)
            if result is not None:
                processed_job, target_collection, field_path = result
                print(f"Processed job: {processed_job}")
                print(f"Target collection: {target_collection.name}")

                update_query = {field_path: image_hash}
                update_data = {"$set": {"image_uuid": processed_job["image_uuid"]}}

                if target_collection.name not in bulk_operations:
                    bulk_operations[target_collection.name] = []

                bulk_operations[target_collection.name].append(UpdateOne(update_query, update_data))

                if len(bulk_operations[target_collection.name]) >= batch_size:
                    target_collection.bulk_write(bulk_operations[target_collection.name])
                    bulk_operations[target_collection.name] = []


    for collection_name, operations in bulk_operations.items():
        if operations:
            collection = db[collection_name]
            collection.bulk_write(operations)

except Exception as e:
    print(f"Error processing job: {e}")

finally:
    cursor.close()
    client.close()

print("Data migrated successfully.")