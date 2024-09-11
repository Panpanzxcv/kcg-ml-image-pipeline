import os
import sys
base_directory = "./"
sys.path.insert(0, base_directory)
import subprocess
import zipfile
from datetime import datetime
from minio import Minio
import logging
from utility.minio import cmd

# MongoDB connection URL
MONGO_URL = "mongodb://192.168.3.1:32017/"

# MinIO credentials and settings
MINIO_ACCESS_KEY = 'v048BpXpWrsVIHUfdAix'
MINIO_SECRET_KEY = '4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu'
MINIO_ENDPOINT = '192.168.3.5:9000'
MINIO_BUCKET = 'db-backup'

# Get current date for naming the backup file
current_date = datetime.now().strftime('%m-%d-%Y')
backup_dir = f"/backup/mongodb-backup-{current_date}"
backup_zip = f"/backup/mongodb-backup-{current_date}.zip"
log_file = "/backup/backup.log"

# Create backup directory if it doesn't exist
os.makedirs(backup_dir, exist_ok=True)

# Function to zip the backup folder
def zip_backup_folder(source_folder, output_filename):
    with zipfile.ZipFile(output_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                zipf.write(os.path.join(root, file),
                           os.path.relpath(os.path.join(root, file), os.path.join(source_folder, '..')))
    print(f"Backup compressed into: {output_filename}")

# Function to run the backup for specific collections
def backup_mongo(database, collections=None):
    try:
        # Base mongodump command
        cmd = ["mongodump", "--uri", MONGO_URL, "--db", database, "--out", backup_dir]

        # If specific collections are provided, back them up
        if collections:
            for collection in collections:
                subprocess.run(
                    cmd + ["--collection", collection],
                    capture_output=True,
                    text=True,
                    check=True
                )
                with open(log_file, "a") as log:
                    log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Backup successful for collection '{collection}' in database '{database}'. Folder: {backup_dir}\n")
        else:
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            with open(log_file, "a") as log:
                log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Backup successful for database '{database}'. Folder: {backup_dir}\n")

        # Compress the backup folder into a zip file
        zip_backup_folder(backup_dir, backup_zip)
        print("Backup and compression successful.")

    except subprocess.CalledProcessError as e:
        with open(log_file, "a") as log:
            log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Backup failed. Error: {e}\n")
        print(f"Backup failed. Error: {e}")

# Run the backup function
if __name__ == "__main__":
    # Specify the database and collections you want to back up
    database_name = "orchestration-job-db"  
    collections_to_backup = ["ranking_datapoints", 
                             "image_tags", 
                             "users", 
                             "completed-jobs",
                              "completed-inpainting-jobs",
                               "all-images",
                               "buckets",
                                "datasets",
                                 "rank_definitions",
                                  "rank_categories",
                                   "tag_definitions"
                                    "tag_categories",
                                     "external_images",
                                      "extracts",
                                       "classifier_models",
                                        "ranking_models",
                                         "irrelevant_images",
                                          "rank_pairs",
                                           "rank_active_learning_policy" ]  # These are the specific collections

    # Call the backup function for the specified collections
    backup_mongo(database_name, collections_to_backup)

    # MinIO client initialization
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=True  # Set to False if MinIO is not using HTTPS
    )

    # Upload the compressed backup to MinIO
    minio_object_name = f"mongodb-backup-{current_date}.zip"
    cmd.upload_from_file(minio_client, MINIO_BUCKET, minio_object_name, backup_zip)
