from pymongo import MongoClient

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"
DATABASE_NAME = "orchestration-job-db"
COLLECTION_NAME = "image_tags"

# Connect to MongoDB
try:
    print("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    image_tags_collection = db[COLLECTION_NAME]
    print("Connected to MongoDB.")

    # Remove the image_uuid field from all documents in the image_tags collection
    print("Removing 'image_uuid' field from all documents in the image_tags collection...")
    result = image_tags_collection.update_many(
        {},
        {"$unset": {"image_uuid": ""}}
    )

    print(f"Successfully removed 'image_uuid' field from {result.modified_count} documents.")
    
except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the connection to MongoDB
    client.close()
    print("MongoDB connection closed.")
