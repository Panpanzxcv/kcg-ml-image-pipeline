from pymongo import MongoClient
from minio import Minio

def get_existing_hashes(db):
    # Fetch all image hashes from completed_jobs_collection, extracts_collection, and external_images_collection
    completed_jobs_hashes = set(db.completed_jobs_collection.distinct("task_output_file_dict.output_file_hash"))
    extracts_hashes = set(db.extracts_collection.distinct("image_hash"))
    external_images_hashes = set(db.external_images_collection.distinct("image_hash"))

    # Combine all hashes
    all_existing_hashes = completed_jobs_hashes.union(extracts_hashes).union(external_images_hashes)
    return all_existing_hashes

def remove_orphaned_entries(collection, all_existing_hashes, hash_field, minio_client):
    orphaned_docs = collection.find({hash_field: {"$nin": list(all_existing_hashes)}})
    
    orphaned_count = orphaned_docs.count()
    
    if orphaned_count > 0:
        print(f"Removing {orphaned_count} orphaned documents from {collection.name}...")
        
        for doc in orphaned_docs:
            # Remove the corresponding file from MinIO
            file_path = doc.get("file_path") or doc.get("task_output_file_dict", {}).get("output_file_path")
            if file_path:
                bucket_name, object_name = file_path.split('/', 1)
                try:
                    # Remove the primary file
                    minio_client.remove_object(bucket_name, object_name)
                    print(f"Removed {object_name} from {bucket_name}")

                    # Remove associated files (for completed jobs, extracts, and external images)
                    associated_files = [
                        f"{object_name.rsplit('.', 1)[0]}_clip_kandinsky.msgpack",
                        f"{object_name.rsplit('.', 1)[0]}_clip.msgpack",
                        f"{object_name.rsplit('.', 1)[0]}_data.msgpack",
                        f"{object_name.rsplit('.', 1)[0]}_embedding.msgpack",
                        f"{object_name.rsplit('.', 1)[0]}_vae_latent.msgpack",
                    ]

                    # Adjust the list of associated files based on the type of document
                    if collection.name == "extracts_collection":
                        associated_files = [
                            f"{object_name.rsplit('.', 1)[0]}_clip_kandinsky.msgpack",
                            f"{object_name.rsplit('.', 1)[0]}_vae_latent.msgpack",
                        ]
                    elif collection.name == "external_images_collection":
                        associated_files = [
                            f"{object_name.rsplit('.', 1)[0]}_clip_kandinsky.msgpack"
                        ]

                    for file in associated_files:
                        minio_client.remove_object(bucket_name, file)
                        print(f"Removed {file} from {bucket_name}")
                except Exception as e:
                    print(f"Failed to remove {object_name} from {bucket_name}: {e}")

        # Remove orphaned documents from MongoDB
        collection.delete_many({hash_field: {"$nin": list(all_existing_hashes)}})
    else:
        print(f"No orphaned documents found in {collection.name}.")

def main():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['your_database_name']  # Replace with your database name

    # Connect to MinIO
    minio_client = Minio(
        "play.min.io",  # Replace with your MinIO server address
        access_key="your-access-key",  # Replace with your access key
        secret_key="your-secret-key",  # Replace with your secret key
        secure=False
    )

    # Get all existing hashes
    all_existing_hashes = get_existing_hashes(db)

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
