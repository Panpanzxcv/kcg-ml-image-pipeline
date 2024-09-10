from pymongo import MongoClient, UpdateOne

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"  # Replace with your MongoDB URI
DATABASE_NAME = "orchestration-job-db"       # Replace with your database name
COMPLETED_JOBS_COLLECTION = "completed-jobs"
EXTERNAL_IMAGES_COLLECTION = "external_images"
EXTRACTS_COLLECTION = "extracts"
ALL_IMAGES_COLLECTION = "all-images"
IMAGE_RANK_SCORES_COLLECTION = "image_rank_scores"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
completed_jobs_collection = db[COMPLETED_JOBS_COLLECTION]
external_images_collection = db[EXTERNAL_IMAGES_COLLECTION]
extracts_collection = db[EXTRACTS_COLLECTION]
all_images_collection = db[ALL_IMAGES_COLLECTION]
image_rank_scores_collection = db[IMAGE_RANK_SCORES_COLLECTION]

def get_bucket_id_for_image_source(image_source: str) -> int:
    if image_source == 'generated_image':
        return 0
    elif image_source == 'extract_image':
        return 1
    elif image_source == 'external_image':
        return 2
    else:
        raise ValueError("Invalid image_source value")

def get_extra_data(image_hash: str, job_uuid: str, bucket_id: int):
    if not image_hash:
        if bucket_id == 0:
            job_data = completed_jobs_collection.find_one({"uuid": job_uuid}, {"task_output_file_dict.output_file_hash": 1})
            if not job_data:
                return None
            
            image_hash = job_data.get("task_output_file_dict", {}).get("output_file_hash")
        if bucket_id == 1:
            job_data = extracts_collection.find_one({"uuid": job_uuid}, {"image_hash": 1})
            if not job_data:
                return None
            
            image_hash = job_data.get("image_hash")
        if bucket_id == 2:
            job_data = external_images_collection.find_one({"uuid": job_uuid}, {"image_hash": 1})
            if not job_data:
                return None
            
            image_hash = job_data.get("image_hash")

    if not image_hash:
        return None

    return all_images_collection.find_one({"image_hash": image_hash, "bucket_id": bucket_id}, {"image_hash": 1, "uuid": 1, "bucket_id": 1, "dataset_id": 1})
    

def add_extra_data_to_rank_scores():
    bulk_operations = []
    batch_size = 1000  # Adjust batch size as needed
    cursor = image_rank_scores_collection.find(no_cursor_timeout=True).batch_size(batch_size)

    print("")
    print("// Starting operations")
    print("")
    
    try:
        for rank_score in cursor:
            image_hash = rank_score.get("image_hash")
            job_uuid = rank_score.get("uuid")
            image_source = rank_score.get("image_source")

            try:
                bucket_id = get_bucket_id_for_image_source(image_source)
            except Exception as e:
                print(f"It was not possible to get the bucket id of the image with uuid {job_uuid} and image source {image_source}")
                continue

            if "image_uuid" in rank_score and "dataset_id" in rank_score:
                print(f"Skipping document with _id: {rank_score['_id']} as it already has image_uuid and dataset_id.")
                continue  # Skip if image_uuid is already present

            extra_data = get_extra_data(image_hash, job_uuid, bucket_id)

            if not extra_data:
                print(f"Skipping document with _id: {rank_score['_id']} as it was not possible to found the extra data for it.")
                continue  # Skip if image_uuid is already present

            if extra_data.get("image_hash") == None or extra_data.get("uuid") == None or extra_data.get("bucket_id") == None or extra_data.get("dataset_id") == None:
                print(f"Skipping document with _id: {rank_score['_id']} as it has incomplete extra data.")
                continue  # Skip if image_uuid is already present
                
            # Prepare the update operation
            extra_data.pop('_id', None)
            extra_data["image_uuid"] = extra_data["uuid"]
            extra_data.pop('uuid', None)
            update_query = {"_id": rank_score["_id"]}
            update_data = {"$set": extra_data}
            bulk_operations.append(UpdateOne(update_query, update_data))

            if len(bulk_operations) >= batch_size:
                # Execute bulk update
                print(f"Executing bulk update for {len(bulk_operations)} documents.")
                image_rank_scores_collection.bulk_write(bulk_operations)
                bulk_operations = []

        if bulk_operations:
            # Execute remaining bulk update
            print(f"Executing final bulk update for {len(bulk_operations)} documents.")
            image_rank_scores_collection.bulk_write(bulk_operations)

        print("")
        print("// Finishing operations")
        print("")

        print("Successfully updated all documents in image_rank_scores_collection.")
    
    except Exception as e:
        print(f"Error during update: {e}")
    
    finally:
        cursor.close()

if __name__ == "__main__":
    add_extra_data_to_rank_scores()
    client.close()
