from google.cloud import bigquery
import os

def load_csv_to_bq(bucket_name, file_name, dataset_id, table_id, schema):
    """Loads a CSV file from GCS into a BigQuery table."""
    client = bigquery.Client()
    
    table_ref = client.dataset(dataset_id).table(table_id)
    uri = f"gs://{bucket_name}/{file_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Assumes first row is headers
        autodetect=False,  # Set to False to define schema explicitly
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    print(f"Loading {file_name} into {dataset_id}.{table_id}...")
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

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
        load_csv_to_bq(bucket_name, table["file_name"], dataset_id, table["table_id"], table["schema"])

if __name__ == "__main__":
    main()
