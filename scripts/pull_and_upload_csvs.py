import requests
import os
from google.cloud import storage

def main():
    # The raw URLs of your CSVs:
    base_url = "https://raw.githubusercontent.com/ae-wizard/ae-sample-project/main/models/"
    files = ["registrations.csv", "transactions.csv", "sessions.csv"]

    # GCS bucket name
    bucket_name = "ae-class-raw"

    # Initialize GCS client (assumes creds are set via env var or attached service account)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for filename in files:
        url = base_url + filename

        print(f"Downloading {url} ...")
        response = requests.get(url)
        response.raise_for_status()  # raises an error if not 200

        print(f"Uploading {filename} to gs://{bucket_name}/{filename} ...")
        blob = bucket.blob(filename)
        blob.upload_from_string(response.content, content_type="text/csv")

    print("Done uploading all CSVs to GCS.")

if __name__ == "__main__":
    main()
