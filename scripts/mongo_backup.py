import os
import subprocess
from datetime import datetime

# MongoDB connection URL
MONGO_URL = "mongodb://192.168.3.1:32017/"

# Backup directory with timestamp in the name
timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
backup_dir = f"/backup/{timestamp}"
log_file = "/backup/backup.log"

# Create backup directory if it doesn't exist
os.makedirs(backup_dir, exist_ok=True)

# Function to run the backup
def backup_mongo():
    try:
        # Run mongodump command
        result = subprocess.run(
            ["mongodump", "--uri", MONGO_URL, "--out", backup_dir],
            capture_output=True,
            text=True,
            check=True
        )

        # Log success
        with open(log_file, "a") as log:
            log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Backup successful. Folder: {backup_dir}\n")
        print("Backup successful.")
    except subprocess.CalledProcessError as e:
        # Log failure
        with open(log_file, "a") as log:
            log.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Backup failed. Error: {e}\n")
        print(f"Backup failed. Error: {e}")

# Run the backup function
if __name__ == "__main__":
    backup_mongo()
