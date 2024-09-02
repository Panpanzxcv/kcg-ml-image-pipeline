from pymongo import MongoClient, UpdateOne
from minio import Minio
from minio.error import S3Error

def process_collection(collection, minio_client, all_existing_hashes, hash_field):
    """
    Process each collection in batches to avoid DocumentTooLarge errors.
    """
    batch_size = 500
    orphaned_docs_cursor = collection.find({hash_field: {"$nin": list(all_existing_hashes)}}).batch_size(batch_size)
    orphaned_count = collection.count_documents({hash_field: {"$nin": list(all_existing_hashes)}})

    if orphaned_count > 0:
        print(f"Removing {orphaned_count} orphaned documents from {collection.name}...")

        for doc in orphaned_docs_cursor:
            if collection.name == "all-images":
                file_path = doc.get("file_path") or doc.get("task_output_file_dict", {}).get("output_file_path")
                if file_path:
                    try:
                        bucket_name, object_name = file_path.split('/', 1)
                        delete_files_from_minio(minio_client, bucket_name, object_name)
                    except ValueError:
                        print(f"Error processing file path: {file_path}")

        # Remove the orphaned documents
        collection.delete_many({hash_field: {"$nin": list(all_existing_hashes)}})
    else:
        print(f"No orphaned documents found in {collection.name}.")

def get_existing_hashes(db):
    completed_jobs_hashes = set()

    # Fetch hashes only from completed-jobs where task_input_dict.dataset exists
    print(f"Fetching hashes from completed-jobs where task_input_dict.dataset exists...")
    try:
        cursor = db["completed-jobs"].find({"task_input_dict.dataset": {"$exists": True}}, 
                                           {"task_output_file_dict.output_file_hash": 1})
        for doc in cursor:
            hash_value = doc.get("task_output_file_dict", {}).get("output_file_hash")
            if hash_value:
                completed_jobs_hashes.add(hash_value)
    except Exception as e:
        print(f"Error fetching from completed-jobs: {e}")

    print(f"Total combined hashes: {len(completed_jobs_hashes)}")
    return completed_jobs_hashes

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
                continue
            else:
                raise e
        except Exception as e:
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

    all_existing_hashes = get_existing_hashes(db)
    print(f"Total existing hashes: {len(all_existing_hashes)}")

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
        db.ranking_datapoints_collection,
    ]

    for collection in collections_to_remove:
        hash_field = "image_hash"
        if collection.name == "irrelevant_images_collection":
            hash_field = "file_hash"

        process_collection(collection, minio_client, all_existing_hashes, hash_field)

    print("Cleanup completed.")

if __name__ == "__main__":
    main()
