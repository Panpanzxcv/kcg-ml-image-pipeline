from pymongo import MongoClient
from minio import Minio
from minio.error import S3Error

def process_collection(collection_name, minio_client, db, hash_field, all_existing_hashes):
    """
    Iterate through each document in the collection and remove it if the hash does not exist
    in the precompiled set of all existing hashes.
    """
    collection = db[collection_name]
    orphaned_count = 0
    ids_to_remove = []

    print(f"Starting to process collection: {collection_name}")
    total_docs = collection.count_documents({})
    print(f"Total documents in {collection_name}: {total_docs}")
    
    cursor = collection.find({}, {hash_field: 1})
    processed_docs = 0
    
    for doc in cursor:
        processed_docs += 1
        doc_hash = doc.get(hash_field)
        if not doc_hash:
            print(f"Document with _id: {doc['_id']} skipped due to missing {hash_field}")
            continue
        

        # Check if the hash exists in the precompiled set of all existing hashes
        if doc_hash not in all_existing_hashes:
            orphaned_count += 1
            print(f"Orphaned document found with {hash_field}: {doc_hash}")
            file_path = doc.get("file_path") or doc.get("task_output_file_dict", {}).get("output_file_path")
            if file_path:
                print(f"Processing orphaned document with file_path: {file_path}")
                if collection_name == "all-images":
                    try:
                        bucket_name, object_name = file_path.split('/', 1)
                        delete_files_from_minio(minio_client, bucket_name, object_name)
                    except ValueError:
                        print(f"Error processing file path: {file_path}")

            # Add the document's ID to the list of IDs to remove
            ids_to_remove.append(doc["_id"])

    # Bulk remove orphaned documents after processing all documents
    if ids_to_remove:
        collection.delete_many({"_id": {"$in": ids_to_remove}})
        print(f"Removed {orphaned_count} documents from {collection_name}")

    print(f"Processed {processed_docs} documents in {collection_name}")
    print(f"Total orphaned documents removed from {collection_name}: {orphaned_count}")

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

def get_all_existing_hashes(db):
    completed_jobs_hashes = set()
    extracts_hashes = set()
    external_images_hashes = set()

    # Fetch hashes from collections in smaller batches
    for collection_name, hash_key in [
        ("completed-jobs", "task_output_file_dict.output_file_hash"),
        ("extracts", "image_hash"),
        ("external_images", "image_hash"),
    ]:
        print(f"Fetching hashes from {collection_name}...")

        # For 'completed-jobs', filter documents where 'task_input_dict.dataset' exists
        if collection_name == "completed-jobs":
            query = {"task_input_dict.dataset": {"$exists": True}}
        else:
            query = {}

        # Initialize a counter for documents in the collection
        doc_count = 0

        cursor = db[collection_name].find(query, {hash_key: 1}).batch_size(1000)
        
        for doc in cursor:
            hash_value = doc.get(hash_key)
            if hash_value:
                doc_count += 1
                if collection_name == "completed-jobs":
                    completed_jobs_hashes.add(hash_value)
                elif collection_name == "extracts":
                    extracts_hashes.add(hash_value)
                elif collection_name == "external_images":
                    external_images_hashes.add(hash_value)

        # Print the number of documents fetched from the current collection
        print(f"Total documents fetched from {collection_name}: {doc_count}")

    # Combine all the collected hashes into one set
    all_existing_hashes = completed_jobs_hashes.union(extracts_hashes).union(external_images_hashes)
    print(f"Total combined hashes: {len(all_existing_hashes)}")
    return all_existing_hashes


def main():
    client = MongoClient("mongodb://192.168.3.1:32017/")
    db = client["orchestration-job-db"]

    minio_client = Minio(
        "192.168.3.5:9000",
        access_key="v048BpXpWrsVIHUfdAix",
        secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu",
        secure=False
    )

    # Get all existing hashes from the primary collections
    all_existing_hashes = get_all_existing_hashes(db)
    print(f"Total existing hashes: {len(all_existing_hashes)}")

    collections_to_remove = [
        "image_tags",
        "all-images",
        "image_rank_scores",
        "image_classifier_scores",
        "image-sigma-scores",
        "image-residuals",
        "image-percentiles",
        "image-residual-percentiles",
        "image-rank-use-count",
        "image_pair_ranking",
        "irrelevant_images",
        "image_hashes",
        "ranking_datapoints"
    ]

    for collection_name in collections_to_remove:
        hash_field = "image_hash"
        if collection_name == "irrelevant_images":
            hash_field = "file_hash"

        process_collection(collection_name, minio_client, db, hash_field, all_existing_hashes)

    print("Cleanup completed.")

if __name__ == "__main__":
    main()
