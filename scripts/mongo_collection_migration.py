from pymongo import MongoClient
from pymongo.errors import PyMongoError

# MongoDB connection URL
mongo_url = "mongodb://192.168.3.1:32017/"
database_name = "orchestration-job-db"  # Replace with your actual database name

# Initialize MongoDB client
client = MongoClient(mongo_url)

# Access the database
db = client[database_name]

# Define old and new collections
old_collection_name = "rank_definitions"
new_collection_name = "rank_collection"

# Access the old and new collections
old_collection = db[old_collection_name]
new_collection = db[new_collection_name]

def migrate_data():
    try:
        # Count documents in old collection
        old_count = old_collection.count_documents({})
        print(f"Found {old_count} documents in the old collection.")

        if old_count == 0:
            print("No documents found in the old collection. Migration not needed.")
            return

        # Copy documents from old collection to new collection
        documents = old_collection.find()
        new_collection.insert_many(documents)
        print(f"Successfully copied {old_count} documents to the new collection.")

        # Optionally verify the data
        new_count = new_collection.count_documents({})
        print(f"New collection now contains {new_count} documents.")
        if old_count == new_count:
            print("Data migration verified successfully. Counts match.")
        else:
            print("Data migration verification failed. Counts do not match.")

    except PyMongoError as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the MongoDB connection
        client.close()

if __name__ == "__main__":
    migrate_data()
