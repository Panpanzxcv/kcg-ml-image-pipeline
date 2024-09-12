from minio import Minio

# Minio connection
minio_client = Minio(
    "192.168.3.5:9000",  
    access_key="v048BpXpWrsVIHUfdAix",  
    secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu", 
    secure=False  # Set to True if using HTTPS
)

# List all top-level datasets in the external bucket
def list_datasets(bucket_name):
    datasets = set()  # Use a set to avoid duplicates
    objects = minio_client.list_objects(bucket_name, recursive=False)
    for obj in objects:
        top_level_folder = obj.object_name.split('/')[0]
        datasets.add(top_level_folder)
    return datasets

# Print datasets that do not contain the clip_vectors folder
def print_datasets_without_clip_vectors():
    bucket_name = 'extracts'
    datasets = list_datasets(bucket_name)
    datasets_without_clip_vectors = []

    for dataset in datasets:
        # Build the path to the specific clip_vectors msgpack file
        file_path = f"{dataset}/clip_vectors/0001_clip_data.msgpack"
        try:
            # Check if the clip_vectors file exists
            minio_client.stat_object(bucket_name, file_path)
        except Exception:
            # If file doesn't exist, add the dataset to the list
            datasets_without_clip_vectors.append(dataset)

    # Print the datasets without clip_vectors folder
    print("Datasets without clip_vectors folder:")
    for dataset in datasets_without_clip_vectors:
        print(dataset)

# Example usage
print_datasets_without_clip_vectors()
