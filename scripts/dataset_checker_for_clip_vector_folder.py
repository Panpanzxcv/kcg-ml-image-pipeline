from minio import Minio

# Minio connection
minio_client = Minio(
    "192.168.3.5:9000",  
    access_key="v048BpXpWrsVIHUfdAix",  
    secret_key="4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu", 
    secure=False  # Set to True if using HTTPS
)

# List all datasets and identify those without a clip_vectors folder
def print_datasets_without_clip_vectors(bucket_name):
    datasets = {}
    
    # List all objects in the external bucket
    objects = minio_client.list_objects(bucket_name, recursive=True)
    
    for obj in objects:
        # Extract top-level dataset name
        dataset_name = obj.object_name.split('/')[0]
        
        # Initialize dataset if not already in the dictionary
        if dataset_name not in datasets:
            datasets[dataset_name] = False
        
        # Check if object is part of clip_vectors folder
        if "clip_vectors/" in obj.object_name:
            datasets[dataset_name] = True
    
    # Print datasets that do not contain clip_vectors folder
    print("Datasets without clip_vectors folder:")
    for dataset, has_clip_vectors in datasets.items():
        if not has_clip_vectors:
            print(dataset)

# Example usage
bucket_name = 'external'
print_datasets_without_clip_vectors(bucket_name)
