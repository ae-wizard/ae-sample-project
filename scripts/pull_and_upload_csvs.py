import requests
from google.cloud import storage
import os

def main():
    # Ensure credentials are set
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    # Base URL for the CSV files from the ae-sample-project repository
    base_url = "https://raw.githubusercontent.com/ae-wizard/ae-sample-project/main/models/"
    csv_files = ["registrations.csv", "transactions.csv", "sessions.csv"]

    # Name of the GCS bucket
    bucket_name = "ae-class-raw"

    # Initialize the GCS client using explicit credentials
    client = storage.Client.from_service_account_json(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
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
