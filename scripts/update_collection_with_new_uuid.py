from pymongo import MongoClient, UpdateOne

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"  # Replace with your MongoDB URI
DATABASE_NAME = "orchestration-job-db"       # Replace with your database name
COMPLETED_JOBS_COLLECTION = "all-images"
IMAGE_RANK_SCORES_COLLECTION = "image_rank_scores"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
completed_jobs_collection = db[COMPLETED_JOBS_COLLECTION]
image_rank_scores_collection = db[IMAGE_RANK_SCORES_COLLECTION]

def preload_completed_jobs():
    """
    Preload all image_hash to image_uuid mappings from the completed_jobs_collection.
    """
    return {job["image_hash"]: job["uuid"] for job in completed_jobs_collection.find({}, {"image_hash": 1, "uuid": 1})}

def add_image_uuid_to_rank_scores():
    bulk_operations = []
    batch_size = 1000  # Adjust batch size as needed
    processed_count = 0  # To track the number of processed documents
    
    # Preload completed jobs into memory
    completed_jobs_mapping = preload_completed_jobs()

    cursor = image_rank_scores_collection.find(no_cursor_timeout=True).batch_size(batch_size)
    
    try:
        for rank_score in cursor:
            image_hash = rank_score.get("image_hash")
            if not image_hash:
                continue  # Skip if no image_hash is present

            if "image_uuid" in rank_score:
                continue  # Skip if image_uuid is already present

            image_uuid = completed_jobs_mapping.get(image_hash)
            if image_uuid:
                # Prepare the update operation
                update_query = {"_id": rank_score["_id"]}
                update_data = {"$set": {"image_uuid": image_uuid}}
                bulk_operations.append(UpdateOne(update_query, update_data))
                
            if len(bulk_operations) >= batch_size:
                # Execute bulk update
                image_rank_scores_collection.bulk_write(bulk_operations)
                bulk_operations = []

            # Progress tracking
            processed_count += 1
            if processed_count % 1000 == 0:
                print(f"Processed {processed_count} documents so far.")

        if bulk_operations:
            # Execute remaining bulk update
            image_rank_scores_collection.bulk_write(bulk_operations)

        print(f"Successfully updated all documents in image_rank_scores_collection with image_uuid. Total documents processed: {processed_count}")
    
    except Exception as e:
        print(f"Error during update: {e}")
    
    finally:
        cursor.close()

if __name__ == "__main__":
    add_image_uuid_to_rank_scores()
    client.close()
