from pymongo import MongoClient
from orchestration.api.utils.uuid64 import Uuid64 

# Initialize the MongoDB client and the users collection
client = MongoClient('mongodb://192.168.3.1:32017/')
db = client['orchestration-job-db']  
users_collection = db['users']  

def update_all_users_with_uuid():
    # Find all users in the collection
    all_users = users_collection.find({})

    updated_count = 0

    # Iterate over each user and update the 'uuid' field
    for user in all_users:
        new_uuid = Uuid64.create_new_uuid()  # Generate new UUID
        users_collection.update_one(
            {"_id": user["_id"]},  # Filter to update the correct user by ID
            {"$set": {"uuid": new_uuid.to_mongo_value()}}  # Set the new 'uuid' field
        )
        updated_count += 1
        print(f"Updated user: {user['username']} with uuid: {new_uuid.to_mongo_value()}")

    print(f"Total users updated: {updated_count}")

if __name__ == "__main__":
    update_all_users_with_uuid()