from pymongo import MongoClient
from minio import Minio
from minio.error import S3Error

def process_collection(collection, minio_client, db, hash_field):
    """
    Iterate through each document in the collection and remove it if the hash does not exist
    in the completed-jobs, extracts, or external_images collections.
    """
    orphaned_count = 0

    print(f"Starting to process collection: {collection.name}")
    
    cursor = collection.find({}, {hash_field: 1})
    
    for doc in cursor:
        doc_hash = doc.get(hash_field)
        if not doc_hash:
            print(f"Document {_id} skipped due to missing {hash_field}")
            continue
        
        print(f"Checking document with {hash_field}: {doc_hash}")

        # Check if the hash exists in any of the primary collections
        in_completed_jobs = db["completed-jobs"].find_one({"task_output_file_dict.output_file_hash": doc_hash})
        in_extracts = db["extracts"].find_one({"image_hash": doc_hash})
        in_external_images = db["external_images"].find_one({"image_hash": doc_hash})

        if in_completed_jobs:
            print(f"Document with {hash_field}: {doc_hash} found in completed-jobs")
        if in_extracts:
            print(f"Document with {hash_field}: {doc_hash} found in extracts")
        if in_external_images:
            print(f"Document with {hash_field}: {doc_hash} found in external_images")
        
        if not (in_completed_jobs or in_extracts or in_external_images):
            orphaned_count += 1
            print(f"Orphaned document found with {hash_field}: {doc_hash}")
            file_path = doc.get("file_path") or doc.get("task_output_file_dict", {}).get("output_file_path")
            if file_path:
                print(f"Processing orphaned document with file_path: {file_path}")
                if collection.name == "all-images":
                    try:
                        bucket_name, object_name = file_path.split('/', 1)
                        delete_files_from_minio(minio_client, bucket_name, object_name)
                    except ValueError:
                        print(f"Error processing file path: {file_path}")

            # Remove the orphaned document
            collection.delete_one({"_id": doc["_id"]})
            print(f"Removed orphaned document with {hash_field}: {doc_hash}")

    print(f"Total orphaned documents removed from {collection.name}: {orphaned_count}")

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

def main():
    client = MongoClient("mongodb://192.168.3.1:32017/")
    db = client["orchestration-job-db"]

    minio_client = Minio(
        "192.168.3.5:9000",
        access_key="v048BpXpWrsVIHUfdAix",
        secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu",
        secure=False
    )

    collections_to_remove = [
        db.image_tags_collection,
        db.all_image_collection,
        db.image_rank_scores_collection,
        db.image_classifier_scores_collection,
        db.image_sigma_scores_collection,
        db.image_residuals_collection,
        db.image_percentiles_collection,
        db.image_residual_percentiles_collection,
        db.image_rank_use_count_collection,
        db.image_pair_ranking_collection,
        db.irrelevant_images_collection,
        db.image_hashes_collection,
    ]

    for collection in collections_to_remove:
        hash_field = "image_hash"
        if collection.name == "irrelevant_images_collection":
            hash_field = "file_hash"

        process_collection(collection, minio_client, db, hash_field)

    print("Cleanup completed.")

if __name__ == "__main__":
    main()
