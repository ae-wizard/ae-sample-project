import requests
from google.cloud import storage
import os

def main():
    # Example URLs for your CSV files in another repo "ae-sample-project"
    base_url = "https://raw.githubusercontent.com/ae-wizard/ae-sample-project/main/models/"
    csv_files = ["registrations.csv", "transactions.csv", "sessions.csv"]

    bucket_name = "ae-class-raw"

    # Instantiate GCS client (auth comes from the service account JSON you'll provide via GitHub Actions)
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
