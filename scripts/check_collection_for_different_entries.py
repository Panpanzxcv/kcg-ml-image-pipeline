from pymongo import MongoClient

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/" 
DATABASE_NAME = "orchestration-job-db"       
COLLECTION_NAME = "image_rank_scores"       

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Define the expected structure
expected_structure = {
    "image_hash": str,
    "rank_model_id": int,
    "uuid": str,
    "creation_time": str,
    "image_source": str,
    "rank_id": int,
    "score": float,
    "sigma_score": float,
    "image_uuid": int
}

def check_document_structure(doc):
    """
    Check if the document matches the expected structure.
    """
    for key, expected_type in expected_structure.items():
        if key not in doc or not isinstance(doc[key], expected_type):
            return False
    return True

def main():
    # Find all documents in the collection
    cursor = collection.find()

    # Count documents that don't match the expected structure
    invalid_count = 0
    for doc in cursor:
        if not check_document_structure(doc):
            invalid_count += 1

    print(f"Number of documents with a different format than expected: {invalid_count}")

if __name__ == "__main__":
    main()
