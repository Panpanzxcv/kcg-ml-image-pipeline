from pymongo import MongoClient, UpdateOne

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"  # Replace with your MongoDB URI
DATABASE_NAME = "orchestration-job-db"       # Replace with your database name
COMPLETED_JOBS_COLLECTION = "all-images"
IMAGE_TAGS_COLLECTION = "image_tags"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
completed_jobs_collection = db[COMPLETED_JOBS_COLLECTION]
image_tags_collection = db[IMAGE_TAGS_COLLECTION]

def add_image_uuid_to_image_tags():
    bulk_operations = []
    batch_size = 10000  # Adjust batch size as needed
    cursor = image_tags_collection.find(no_cursor_timeout=True).batch_size(batch_size)
    
    try:
        for image_tag in cursor:
            image_hash = image_tag.get("image_hash")
            image_source = image_tag.get("image_source")

            if not image_hash or not image_source:
                print("Skipping document without image_hash or image_source.")
                continue  # Skip if no image_hash or image_source is present

            if "image_uuid" in image_tag:
                print(f"Skipping document with _id: {image_tag['_id']} as it already has image_uuid.")
                continue  # Skip if image_uuid is already present

            # Find the corresponding document in completed_jobs_collection
            job_data = completed_jobs_collection.find_one({"image_hash": image_hash, "image_source": image_source}, {"uuid": 1})
            if job_data and "uuid" in job_data:
                image_uuid = job_data["uuid"]
                
                # Prepare the update operation
                update_query = {"_id": image_tag["_id"]}
                update_data = {"$set": {"image_uuid": image_uuid}}
                bulk_operations.append(UpdateOne(update_query, update_data))

            else:
                print(f"No matching job found for image_hash: {image_hash} and image_source: {image_source}")

            if len(bulk_operations) >= batch_size:
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
