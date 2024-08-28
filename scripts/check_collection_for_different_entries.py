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
    # Counters for various conditions
    invalid_structure_count = 0
    dataset_field_count = 0
    model_id_field_count = 0
    missing_rank_model_id_count = 0
    missing_rank_id_count = 0

    # Find all documents in the collection
    cursor = collection.find()

    for doc in cursor:
        if not check_document_structure(doc):
            invalid_structure_count += 1
        
        if 'dataset' in doc:
            dataset_field_count += 1
        
        if 'model_id' in doc:
            model_id_field_count += 1
        
        if 'rank_model_id' not in doc:
            missing_rank_model_id_count += 1
        
        if 'rank_id' not in doc:
            missing_rank_id_count += 1

    # Print out the results
    print(f"Number of documents with a different format than expected: {invalid_structure_count}")
    print(f"Number of documents with a 'dataset' field: {dataset_field_count}")
    print(f"Number of documents with a 'model_id' field: {model_id_field_count}")
    print(f"Number of documents missing 'rank_model_id': {missing_rank_model_id_count}")
    print(f"Number of documents missing 'rank_id': {missing_rank_id_count}")

if __name__ == "__main__":
    main()
