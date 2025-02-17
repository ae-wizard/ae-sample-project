import io
import csv
import ast
import json
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

def fix_session_metadata(value):
    """
    Convert the session_metadata field to valid JSON.
    If the value is a string representing a dictionary with single quotes,
    this function will parse it and re-serialize it to a proper JSON string.
    """
    if not value:
        return value
    try:
        # If the value is already valid JSON, this will succeed.
        json.loads(value)
        return value
    except Exception:
        try:
            # Attempt to interpret the value as a Python literal (e.g. a dict with single quotes)
            parsed = ast.literal_eval(value)
            return json.dumps(parsed)
        except Exception as e:
            print(f"Error processing session_metadata '{value}': {e}")
            return value

def load_csv_to_bq(bucket_name, file_name, dataset_id, table_id, schema, max_bad_records=0, process_csv=False):
    """
    Loads a CSV file from GCS into a BigQuery table.
    If process_csv is True, the CSV file is processed in memory (e.g., to fix timestamps and session_metadata).
    """
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    
    if process_csv:
        # Download the file from GCS and process it (for sessions.csv)
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        contents = blob.download_as_text()
        
        input_io = io.StringIO(contents)
        reader = csv.reader(input_io)
        header = next(reader)
        
        # Identify columns to fix timestamps and session metadata
        try:
            start_idx = header.index("visit_start_time")
            end_idx = header.index("visit_end_time")
        except ValueError as e:
            raise ValueError("Required timestamp columns not found in the header.") from e

        try:
            metadata_idx = header.index("session_metadata")
        except ValueError:
            metadata_idx = None

        output_io = io.StringIO()
        writer = csv.writer(output_io)
        writer.writerow(header)
        
        for row in reader:
            row[start_idx] = fix_timestamp(row[start_idx])
            row[end_idx] = fix_timestamp(row[end_idx])
            if metadata_idx is not None:
                row[metadata_idx] = fix_session_metadata(row[metadata_idx])
            writer.writerow(row)
        
        processed_csv = output_io.getvalue()
        processed_io = io.StringIO(processed_csv)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=max_bad_records  # Now expect no errors
        )
        
        print(f"Loading processed {file_name} into {dataset_id}.{table_id} ...")
        load_job = client.load_table_from_file(processed_io, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id}")
    else:
        uri = f"gs://{bucket_name}/{file_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            max_bad_records=max_bad_records
        )
        print(f"Loading {file_name} into {dataset_id}.{table_id} ...")
        load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
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
                bigquery.SchemaField("registered_at", "TIMESTAMP"),
            ],
            "process_csv": False,
            "max_bad_records": 0,
        },
        {
            "file_name": "sessions.csv",
            "table_id": "sessions",
            "schema": [
                bigquery.SchemaField("visit_id", "STRING"),
                bigquery.SchemaField("visit_start_time", "TIMESTAMP"),
                bigquery.SchemaField("visit_end_time", "TIMESTAMP"),
                bigquery.SchemaField("device_type", "STRING"),
                bigquery.SchemaField("browser", "STRING"),
                bigquery.SchemaField("pageview_count", "INT64"),
                bigquery.SchemaField("spend_type", "STRING"),
                bigquery.SchemaField("attributed_channel", "STRING"),
                bigquery.SchemaField("attributed_subchannel", "STRING"),
                bigquery.SchemaField("session_metadata", "JSON"),  # JSON type now
            ],
            "process_csv": True,  # Process CSV to fix timestamps and session_metadata
            "max_bad_records": 0,
        },
        {
            "file_name": "transactions.csv",
            "table_id": "transactions",
            "schema": [
                bigquery.SchemaField("order_id", "STRING"),
                bigquery.SchemaField("visit_id", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("order_created_at", "TIMESTAMP"),
                bigquery.SchemaField("location_id", "STRING"),
                bigquery.SchemaField("shipping_carrier", "STRING"),
                bigquery.SchemaField("shipping_method", "STRING"),
                bigquery.SchemaField("estimated_delivery_date", "TIMESTAMP"),
                bigquery.SchemaField("delivered_at", "TIMESTAMP"),
            ],
            "process_csv": False,
            "max_bad_records": 0,
        },
    ]

    for table in tables:
        load_csv_to_bq(
            bucket_name,
            table["file_name"],
            dataset_id,
            table["table_id"],
            table["schema"],
            max_bad_records=table.get("max_bad_records", 0),
            process_csv=table.get("process_csv", False)
        )

if __name__ == "__main__":
    main()
