import io
import csv
from google.cloud import bigquery, storage

def fix_timestamp(ts):
    """
    Fix a timestamp string to ensure the hour is two digits.
    Expected input: "YYYY-MM-DD H:MM:SS" or "YYYY-MM-DD HH:MM:SS"
    Returns: "YYYY-MM-DD HH:MM:SS"
    """
    if not ts:
        return ts
    try:
        date_part, time_part = ts.split(" ")
        hh, mm, ss = time_part.split(":")
        if len(hh) == 1:
            hh = "0" + hh
        return f"{date_part} {hh}:{mm}:{ss}"
    except Exception as e:
        print(f"Error processing timestamp '{ts}': {e}")
        return ts

def load_csv_to_bq(bucket_name, file_name, dataset_id, table_id, schema):
    client_bq = bigquery.Client()
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    client_storage = storage.Client()

    if file_name == "sessions.csv":
        # Download sessions.csv from GCS
        bucket = client_storage.bucket(bucket_name)
        blob = bucket.blob(file_name)
        contents = blob.download_as_text()

        # Process the CSV to fix timestamp fields
        input_io = io.StringIO(contents)
        reader = csv.reader(input_io)
        header = next(reader)
        
        try:
            start_idx = header.index("visit_start_time_et")
            end_idx = header.index("visit_end_time_et")
        except ValueError as e:
            raise ValueError("Required timestamp columns not found in the header.") from e

        # Prepare corrected CSV in memory
        output_io = io.StringIO()
        writer = csv.writer(output_io)
        writer.writerow(header)

        for row in reader:
            row[start_idx] = fix_timestamp(row[start_idx])
            row[end_idx] = fix_timestamp(row[end_idx])
            writer.writerow(row)

        corrected_csv = output_io.getvalue()
        corrected_io = io.StringIO(corrected_csv)

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header
            autodetect=False,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=0  # Now we expect no errors since we fixed the timestamps
        )

        print(f"Loading corrected {file_name} into {dataset_id}.{table_id} ...")
        load_job = client_bq.load_table_from_file(corrected_io, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

    else:
        # For other files, load directly from GCS URI
        uri = f"gs://{bucket_name}/{file_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=0
        )
        print(f"Loading {file_name} into {dataset_id}.{table_id} ...")
        load_job = client_bq.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")

def main():
    bucket_name = "ae-class-raw"
    dataset_id = "class_source_data"

    tables = [
        {
            "file_name": "registrations.csv",
            "table_id": "registrations",
            "schema": [
                bigquery.SchemaField("visit_id", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("registered_at_et", "TIMESTAMP"),
            ],
        },
        {
            "file_name": "sessions.csv",
            "table_id": "sessions",
            "schema": [
                bigquery.SchemaField("visit_id", "STRING"),
                bigquery.SchemaField("visit_start_time_et", "TIMESTAMP"),
                bigquery.SchemaField("visit_end_time_et", "TIMESTAMP"),
                bigquery.SchemaField("device_type", "STRING"),
                bigquery.SchemaField("browser", "STRING"),
                bigquery.SchemaField("pageview_count", "INT64"),
                bigquery.SchemaField("spend_type", "STRING"),
                bigquery.SchemaField("attributed_channel", "STRING"),
                bigquery.SchemaField("attributed_subchannel", "STRING"),
                bigquery.SchemaField("session_metadata", "STRING"),
            ],
        },
        {
            "file_name": "transactions.csv",
            "table_id": "transactions",
            "schema": [
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("visit_id", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("order_created_at_et", "TIMESTAMP"),
                bigquery.SchemaField("location_id", "STRING"),
                bigquery.SchemaField("shipping_carrier", "STRING"),
                bigquery.SchemaField("shipping_method", "STRING"),
                bigquery.SchemaField("estimated_delivery_date", "TIMESTAMP"),
                bigquery.SchemaField("delivered_at_et", "TIMESTAMP"),
            ],
        },
    ]

    for table in tables:
        load_csv_to_bq(
            bucket_name,
            table["file_name"],
            dataset_id,
            table["table_id"],
            table["schema"]
        )

if __name__ == "__main__":
    main()
