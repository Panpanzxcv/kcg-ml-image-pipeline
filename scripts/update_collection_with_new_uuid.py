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

def add_image_uuid_to_rank_scores():
    bulk_operations = []
    batch_size = 1000  # Adjust batch size as needed
    cursor = image_rank_scores_collection.find(no_cursor_timeout=True).batch_size(batch_size)
    
    try:
        for rank_score in cursor:
            image_hash = rank_score.get("image_hash")
            if not image_hash:
                print("Skipping document without image_hash.")
                continue  # Skip if no image_hash is present

            # Find the corresponding document in completed_jobs_collection
            job_data = completed_jobs_collection.find_one({"image_hash": image_hash}, {"uuid": 1})
            if job_data and "uuid" in job_data:
                image_uuid = job_data["uuid"]
                
                # Prepare the update operation
                update_query = {"_id": rank_score["_id"]}
                update_data = {"$set": {"image_uuid": image_uuid}}
                bulk_operations.append(UpdateOne(update_query, update_data))
                
                print(f"Preparing to update document with _id: {rank_score['_id']}, setting image_uuid: {image_uuid}")

            else:
                print(f"No matching job found for image_hash: {image_hash}")

            if len(bulk_operations) >= batch_size:
                # Execute bulk update
                print(f"Executing bulk update for {len(bulk_operations)} documents.")
                image_rank_scores_collection.bulk_write(bulk_operations)
                bulk_operations = []

        if bulk_operations:
            # Execute remaining bulk update
            print(f"Executing final bulk update for {len(bulk_operations)} documents.")
            image_rank_scores_collection.bulk_write(bulk_operations)

        print("Successfully updated all documents in image_rank_scores_collection with image_uuid.")
    
    except Exception as e:
        print(f"Error during update: {e}")
    
    finally:
        cursor.close()

if __name__ == "__main__":
    add_image_uuid_to_rank_scores()
    client.close()
