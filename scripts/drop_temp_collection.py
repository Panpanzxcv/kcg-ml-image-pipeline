from pymongo import MongoClient

def drop_temp_collection_if_exists(db, collection_name):
    """
    Check if a collection exists in the database and drop it if it does.
    """
    if collection_name in db.list_collection_names():
        print(f"Collection '{collection_name}' exists. Dropping it...")
        db[collection_name].drop()
        print(f"Collection '{collection_name}' has been dropped.")
    else:
        print(f"Collection '{collection_name}' does not exist. No action taken.")

def main():
    # Connect to MongoDB
    client = MongoClient("mongodb://192.168.3.1:32017/")
    db = client["orchestration-job-db"]

    # Drop the temp collection if it exists
    drop_temp_collection_if_exists(db, "temp_hashes")

if __name__ == "__main__":
    main()
