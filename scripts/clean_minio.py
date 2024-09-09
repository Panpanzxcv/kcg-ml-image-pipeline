from minio import Minio
from pymongo import MongoClient
from minio.error import S3Error



# Initialize MinIO client
minio_client = Minio(
    '192.168.3.5:9000',  
    access_key='v048BpXpWrsVIHUfdAix',  
    secret_key='4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu', 
    secure=False  
)

# Initialize MongoDB client
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["your_database"]

# MongoDB collections and MinIO buckets
collections_and_buckets = {
    "extract": {"collection": db.extract_collection, "bucket": "extract"},
    "external": {"collection": db.external_collection, "bucket": "external"},
    "complete": {"collection": db.completed_jobs_collection, "bucket": "datasets"}
}

# Helper function to list files in MinIO bucket
def list_minio_files(bucket_name):
    files = set()
    objects = minio_client.list_objects(bucket_name, recursive=True)
    for obj in objects:
        files.add(obj.object_name)  # Store object names
    return files

# Helper function to list expected files from MongoDB collection
def list_mongo_files(mongo_collection, file_field):
    files = set()
    documents = mongo_collection.find({}, {file_field: 1})
    for doc in documents:
        files.add(doc[file_field])  # Use file_path from MongoDB as the identifier
    return files

# Helper function to delete files from MinIO, including related files
def delete_files_from_minio(minio_client, bucket_name, object_name):
    files_to_delete = [
        object_name,
        f"{object_name.rsplit('.', 1)[0]}_clip_kandinsky.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_clip.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_data.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_embedding.msgpack",
        f"{object_name.rsplit('.', 1)[0]}_vae_latent.msgpack",
    ]

    for file in files_to_delete:
        try:
            minio_client.remove_object(bucket_name, file)
            print(f"Removed {file} from {bucket_name}")
        except S3Error as e:
            if e.code == 'NoSuchKey':
                print(f"File {file} not found in {bucket_name}, skipping removal.")
                continue
            else:
                print(f"Error removing {file} from {bucket_name}: {e}")
                raise e
        except Exception as e:
            print(f"General error removing {file} from {bucket_name}: {e}")
            raise e

# Iterate through each bucket and MongoDB collection
for collection_name, config in collections_and_buckets.items():
    mongo_collection = config["collection"]
    bucket_name = config["bucket"]

    # Get list of files from MongoDB and MinIO
    mongo_files = list_mongo_files(mongo_collection, "file_path")  # 'file_path' contains the file in MinIO
    minio_files = list_minio_files(bucket_name)

    # Find files in MinIO but not in MongoDB
    files_to_delete = minio_files - mongo_files

    # Delete the files and their related files from MinIO
    for file_name in files_to_delete:
        print(f"Removing {file_name} and related files from bucket {bucket_name}")
        delete_files_from_minio(minio_client, bucket_name, file_name)
