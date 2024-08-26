from pymongo import MongoClient, UpdateOne
from minio import Minio
from minio.error import S3Error

def get_existing_hashes(db):
    # Fetch all image hashes from completed_jobs_collection, extracts_collection, and external_images_collection
    completed_jobs_hashes = set(db.completed_jobs_collection.distinct("task_output_file_dict.output_file_hash"))
    extracts_hashes = set(db.extracts_collection.distinct("image_hash"))
    external_images_hashes = set(db.external_images_collection.distinct("image_hash"))

    # Combine all hashes
    all_existing_hashes = completed_jobs_hashes.union(extracts_hashes).union(external_images_hashes)
    return all_existing_hashes

def delete_files_from_minio(minio_client, bucket_name, object_name):
    """
    Delete files from MinIO. This function will attempt to delete the main file
    and associated files. If any file is missing, it will silently continue with the next file.
    """
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
                # Silently continue if the file does not exist
                continue
            else:
                # Raise the exception if it's any other error
                raise e
        except Exception as e:
            # Raise the exception for any other general errors
            raise e

def remove_orphaned_entries(collection, all_existing_hashes, hash_field, minio_client):
    # Use count_documents to get the number of orphaned documents
    orphaned_count = collection.count_documents({hash_field: {"$nin": list(all_existing_hashes)}})
    
    if orphaned_count > 0:
        print(f"Removing {orphaned_count} orphaned documents from {collection.name}...")
        
        # Recreate the cursor since we've counted the documents
        orphaned_docs = collection.find({hash_field: {"$nin": list(all_existing_hashes)}})
        
        for doc in orphaned_docs:
            # Only remove the corresponding file from MinIO if in the all_image_collection
            if collection.name == "all_image_collection":
                file_path = doc.get("file_path") or doc.get("task_output_file_dict", {}).get("output_file_path")
                if file_path:
                    try:
                        bucket_name, object_name = file_path.split('/', 1)
                        delete_files_from_minio(minio_client, bucket_name, object_name)
                    except ValueError:
                        print(f"Error processing file path: {file_path}")

        # Remove orphaned documents from MongoDB
        collection.delete_many({hash_field: {"$nin": list(all_existing_hashes)}})
    else:
        print(f"No orphaned documents found in {collection.name}.")

def main():
    # Connect to MongoDB
    client = MongoClient('mongodb://192.168.3.1:32017/')
    db = client['orchestration-job-db']  

    # Connect to MinIO
    minio_client = Minio(
        "192.168.3.5:9000",  
        access_key="v048BpXpWrsVIHUfdAix",  
        secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu",  
        secure=False
    )

    # Get all existing hashes
    all_existing_hashes = get_existing_hashes(db)
    print(f"Total existing hashes: {len(all_existing_hashes)}")


    # List of collections to clean
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
        db.irrelevant_images_collection,  # This uses "file_hash" instead of "image_hash"
        db.image_hashes_collection,
        db.ranking_datapoints_collection,
    ]

    # Iterate through the collections and remove orphaned documents
    for collection in collections_to_remove:
        hash_field = "image_hash"
        if collection.name == "irrelevant_images_collection":
            hash_field = "file_hash"

        remove_orphaned_entries(collection, all_existing_hashes, hash_field, minio_client)

    print("Cleanup completed.")

if __name__ == "__main__":
    main()
