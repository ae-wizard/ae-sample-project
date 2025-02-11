import requests
from google.cloud import storage
import os

def main():
    # Base URL for the CSV files from the ae-sample-project repository
    base_url = "https://raw.githubusercontent.com/ae-wizard/ae-sample-project/main/models/"
    csv_files = ["registrations.csv", "transactions.csv", "sessions.csv"]

    # Name of the GCS bucket (without the gs:// prefix when using the API)
    bucket_name = "ae-class-raw"

    # Instantiate the GCS client (the credentials are provided via GitHub Actions)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for file in csv_files:
        url = base_url + file
        print(f"Downloading {url}")
        response = requests.get(url)
        response.raise_for_status()

        print(f"Uploading {file} to gs://{bucket_name}/{file}")
        blob = bucket.blob(file)
        blob.upload_from_string(response.content, content_type="text/csv")

    print("All CSVs uploaded successfully!")

if __name__ == "__main__":
    main()
