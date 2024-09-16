import json
from pymongo import MongoClient
from minio import Minio
from io import BytesIO
from minio.error import S3Error
import io
from utility.minio import cmd

# MongoDB connection setup
mongo_client = MongoClient('mongodb://192.168.3.1:32017/')
db = mongo_client['orchestration-job-db']  
collection = db['image_tags']  

# Minio connection setup
minio_client = Minio(
    "192.168.3.5:9000",  
    access_key="v048BpXpWrsVIHUfdAix", 
    secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu", 
    secure=False  
)

bucket_name = "tags"  

# Ensure bucket exists
if not cmd.check_if_bucket_exists(bucket_name):
    cmd.create_bucket(bucket_name)

# Fetch all documents from MongoDB
documents = collection.find()

# Iterate over each document
for doc in documents:
    try:
        # Define file name based on 'tag_id' and 'image_hash'
        file_name = f"{doc['tag_id']}-{doc['tag_type']}-{doc['image_hash']}.json"

        # Convert the MongoDB document to JSON format
        json_data = json.dumps(doc, default=str)

        # Upload the JSON file to Minio
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=file_name,
            data=io.BytesIO(json_data.encode('utf-8')),
            length=len(json_data),
            content_type='application/json'
        )

        print(f"Uploaded {file_name} to Minio.")

    except S3Error as e:
        print(f"Failed to upload {file_name}: {str(e)}")
