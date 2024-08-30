from pymongo import MongoClient, UpdateOne

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"  # Replace with your MongoDB URI
DATABASE_NAME = "orchestration-job-db"       # Replace with your database name
ALL_IMAGES_COLLECTION = "all-images"
IMAGE_TAGS_COLLECTION = "image_tags"
COMPLETED_JOBS_COLLECTION = "completed-jobs"
EXTRACTS_COLLECTION = "extracts"
EXTERNAL_IMAGES_COLLECTION = "external_images"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
all_images_collection = db[ALL_IMAGES_COLLECTION]
image_tags_collection = db[IMAGE_TAGS_COLLECTION]
completed_jobs_collection = db[COMPLETED_JOBS_COLLECTION]
extracts_collection = db[EXTRACTS_COLLECTION]
external_images_collection = db[EXTERNAL_IMAGES_COLLECTION]

def get_bucket_id_and_image_uuid(image_hash):
    """
    Determine the bucket_id by searching for the image_hash in the different collections.
    Returns the bucket_id and the image_uuid if found, otherwise returns None, None.
    """
    # Check in completed_jobs_collection for bucket_id = 0
    job_data = completed_jobs_collection.find_one({"task_output_file_dict.output_file_hash": image_hash}, {"_id": 1})
    if job_data:
        return 0, None

    # Check in extracts_collection for bucket_id = 1
    job_data = extracts_collection.find_one({"image_hash": image_hash}, {"_id": 1})
    if job_data:
        return 1, None

    # Check in external_images_collection for bucket_id = 2
    job_data = external_images_collection.find_one({"image_hash": image_hash}, {"_id": 1})
    if job_data:
        return 2, None

    # If not found in any collection
    return None, None

def find_image_uuid_in_all_images(image_hash, bucket_id):
    """
    Find the image_uuid in the all-images collection based on the image_hash and bucket_id.
    """
    return all_images_collection.find_one({"image_hash": image_hash, "bucket_id": bucket_id}, {"uuid": 1})

def add_image_uuid_to_image_tags():
    bulk_operations = []
    cursor = image_tags_collection.find(no_cursor_timeout=True)

    try:
        for image_tag in cursor:
            image_hash = image_tag.get("image_hash")

            if not image_hash:
                print(f"Skipping document without image_hash.")
                continue  # Skip if no image_hash is present

            # Determine the correct bucket_id by searching in the respective collections
            bucket_id, _ = get_bucket_id_and_image_uuid(image_hash)

            if bucket_id is None:
                print(f"No matching job found for image_hash: {image_hash}")
                continue

            # Find the corresponding image_uuid in the all-images collection
            image_uuid_data = find_image_uuid_in_all_images(image_hash, bucket_id)
            if image_uuid_data and "uuid" in image_uuid_data:
                image_uuid = image_uuid_data["uuid"]

                # Prepare the update operation
                update_query = {"_id": image_tag["_id"]}
                update_data = {"$set": {"image_uuid": image_uuid}}
                bulk_operations.append(UpdateOne(update_query, update_data))
            else:
                print(f"No matching image_uuid found in all-images collection for image_hash: {image_hash} and bucket_id: {bucket_id}")

            if len(bulk_operations) >= 1000:  # Adjust batch size as needed
                # Execute bulk update
                print(f"Executing bulk update for {len(bulk_operations)} documents.")
                image_tags_collection.bulk_write(bulk_operations)
                bulk_operations = []

        if bulk_operations:
            # Execute remaining bulk update
            print(f"Executing final bulk update for {len(bulk_operations)} documents.")
            image_tags_collection.bulk_write(bulk_operations)

        print("Successfully updated all documents in image_tags_collection with image_uuid.")

    except Exception as e:
        print(f"Error during update: {e}")

    finally:
        cursor.close()

if __name__ == "__main__":
    add_image_uuid_to_image_tags()
    client.close()
