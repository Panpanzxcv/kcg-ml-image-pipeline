from pymongo import MongoClient, UpdateOne
from minio import Minio
from io import BytesIO
import json
from collections import OrderedDict

# MongoDB connection details
MONGO_URI = "mongodb://192.168.3.1:32017/"  # Replace with your MongoDB URI
DATABASE_NAME = "orchestration-job-db"       # Replace with your database name
COMPLETED_JOBS_COLLECTION = "all-images"
RANKING_DATAPOINTS_COLLECTION = "ranking_datapoints"

# MinIO connection details
MINIO_ENDPOINT = "192.168.3.5:9000"
MINIO_ACCESS_KEY = "v048BpXpWrsVIHUfdAix"
MINIO_SECRET_KEY = "4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu"
BUCKET_NAME = "datasets"
RANKS_PATH = "ranks"  # Specify the path after the bucket name

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
completed_jobs_collection = db[COMPLETED_JOBS_COLLECTION]
ranking_datapoints_collection = db[RANKING_DATAPOINTS_COLLECTION]

# Initialize MinIO client
minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

def get_bucket_id(image_source):
    """
    Translate image_source to the corresponding bucket_id.
    """
    if image_source == 'generated_image':
        return 0
    elif image_source == 'extract_image':
        return 1
    elif image_source == 'external_image':
        return 2
    else:
        return None

def update_minio_object(file_name, update_data):
    """
    Fetch and update the JSON object on MinIO using the provided file_name under the specified path.
    """
    try:
        # Construct the full path
        path_prefix = f"{RANKS_PATH}/"
        objects = minio_client.list_objects(BUCKET_NAME, prefix=path_prefix, recursive=True)

        for obj in objects:
            # Only process the object if it matches the file_name
            if obj.object_name.endswith(file_name):
                print(f"Processing MinIO object: {obj.object_name}")

                # Get the object
                response = minio_client.get_object(BUCKET_NAME, obj.object_name)
                data = response.read()
                response.close()
                response.release_conn()

                # Convert JSON data to a Python dictionary
                try:
                    json_data = json.loads(data)
                except json.JSONDecodeError:
                    print(f"Skipping non-JSON object: {obj.object_name}")
                    continue

                # Update the JSON data with the new fields
                for key, value in update_data.items():
                    # Navigate into nested dictionaries
                    keys = key.split('.')
                    target_dict = json_data
                    for k in keys[:-1]:
                        target_dict = target_dict.setdefault(k, {})
                    target_dict[keys[-1]] = value

                # Convert back to JSON
                updated_json_data = json.dumps(json_data, indent=4).encode('utf-8')
                updated_data_stream = BytesIO(updated_json_data)

                # Upload the updated object back to MinIO
                minio_client.put_object(
                    BUCKET_NAME,
                    obj.object_name,
                    updated_data_stream,
                    length=len(updated_json_data),
                    content_type='application/json'
                )
                print(f"Updated MinIO object: {obj.object_name}")
                break  # Stop after processing the matching object

    except Exception as e:
        print(f"Error updating MinIO object {file_name}: {e}")

def add_image_uuid_to_ranking_datapoints():
    bulk_operations = []
    batch_size = 1000  # Adjust batch size as needed
    cursor = ranking_datapoints_collection.find(no_cursor_timeout=True).batch_size(batch_size)

    try:
        for datapoint in cursor:
            update_data = {}
            for image_metadata_field in ["image_1_metadata", "image_2_metadata"]:
                image_metadata = datapoint.get(image_metadata_field, {})
                image_hash = image_metadata.get("file_hash")
                image_source = image_metadata.get("image_source")
                bucket_id = get_bucket_id(image_source)

                if not image_hash or bucket_id is None:
                    print(f"Skipping {image_metadata_field} in document with _id: {datapoint['_id']} due to missing image_hash or invalid image_source.")
                    continue  # Skip if no image_hash is present or bucket_id is invalid

                # Find the corresponding document in completed_jobs_collection
                job_data = completed_jobs_collection.find_one({"image_hash": image_hash, "bucket_id": bucket_id}, {"uuid": 1})
                if job_data and "uuid" in job_data:
                    image_uuid = job_data["uuid"]
                    update_data[f"{image_metadata_field}.image_uuid"] = image_uuid
                else:
                    print(f"No matching job found for {image_metadata_field} with image_hash: {image_hash} and bucket_id: {bucket_id}")

            if update_data:
                # Prepare the update operation for MongoDB
                update_query = {"_id": datapoint["_id"]}
                bulk_operations.append(UpdateOne(update_query, {"$set": update_data}))

                # Update the corresponding object in MinIO
                update_minio_object(datapoint["file_name"], update_data)

            if len(bulk_operations) >= batch_size:
                # Execute bulk update for MongoDB
                print(f"Executing bulk update for {len(bulk_operations)} documents.")
                ranking_datapoints_collection.bulk_write(bulk_operations)
                bulk_operations = []

        if bulk_operations:
            # Execute remaining bulk update for MongoDB
            print(f"Executing final bulk update for {len(bulk_operations)} documents.")
            ranking_datapoints_collection.bulk_write(bulk_operations)

        print("Successfully updated all documents in ranking_datapoints_collection with image_uuid.")

    except Exception as e:
        print(f"Error during update: {e}")

    finally:
        cursor.close()

if __name__ == "__main__":
    add_image_uuid_to_ranking_datapoints()
    client.close()
