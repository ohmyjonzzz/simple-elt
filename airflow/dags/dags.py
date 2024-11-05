from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

import os
from dotenv import load_dotenv

load_dotenv()

# BigQuery config variables
GCP_CONN_ID = os.getenv("GCP_CONN_ID")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
GCS_BUCKET = os.getenv("GCS_BUCKET")

# Postgres config variables
PG_CONN_ID = os.getenv("PG_CONN_ID")
PG_SCHEMA = os.getenv("PG_SCHEMA")
PG_TABLE = os.getenv("PG_TABLE")
CSV_FILENAME = os.getenv("CSV_FILENAME")

# Define the default arguments for the DAG
default_args = {
    "owner": "ohmyjons",
    "start_date": datetime.now(),
    "retries": 1,  # Number of retries if a task fails
    "retry_delay": timedelta(minutes=1),  # Time between retries
}

# Create a DAG instance
with DAG(
    dag_id="simple_elt",
    default_args=default_args,
    description="ELT pipeline from PostgreSQL to GCS to BigQuery",
    schedule_interval=None,  # Set the schedule interval (e.g., None for manual runs)
    catchup=False  # Do not backfill (run past dates) when starting the DAG
) as dag:

    # Task to transfer data from PostgreSQL to GCS Bucket
    postgres_to_gcs = PostgresToGCSOperator(
        task_id = "postgres_to_gcs",
        sql = f"SELECT * FROM {PG_TABLE};",
        bucket = GCS_BUCKET,
        filename = CSV_FILENAME,
        export_format = "CSV",  # Can change the export format as needed
        postgres_conn_id = PG_CONN_ID,  # Use Postgres connection ID in Airflow
        field_delimiter=",",  # Optional, specify field delimiter for CSV
        gzip = False,  # Set to True if we want to compress the output file
        gcp_conn_id = GCP_CONN_ID  # Use GCP connection ID in Airflow
    )

    # Task to transfer data from GCS Bucket to BigQuery
    bq_load_csv = GCSToBigQueryOperator(
        task_id="bq_load_csv",
        bucket=GCS_BUCKET,
        source_objects=[CSV_FILENAME],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        schema_fields=[
            {"name": "segment", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "discount_band", "type": "STRING", "mode": "NULLABLE"},
            {"name": "units_sold", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "manufacturing_price", "type": "INT64", "mode": "NULLABLE"},
            {"name": "sale_price", "type": "INT64", "mode": "NULLABLE"},
            {"name": "date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "discount_percentage", "type": "FLOAT64", "mode": "NULLABLE"}    
        ],
        create_disposition="CREATE_IF_NEEDED",  # Can change this if needed
        write_disposition="WRITE_TRUNCATE", # Can change this if needed
        gcp_conn_id = GCP_CONN_ID  # Use GCP connection ID in Airflow
        # skip_leading_rows=1
    )

    # Task BigQueryCreateEmpytyTableOperator: to create an empty table in BigQuery
    bq_create_calculate_view = BigQueryCreateEmptyTableOperator(
        task_id="bq_create_calculate_view",
        dataset_id=BQ_DATASET,
        table_id="calculate_view",
        view={
            "query": f"""
                    SELECT 
                        date, 
                        segment, 
                        country, 
                        product, 
                        discount_band, 
                        discount_percentage, 
                        units_sold, 
                        manufacturing_price, 
                        sale_price, 
                        units_sold * sale_price AS gross_sales, 
                        (units_sold * sale_price) * discount_percentage AS discounts,
                        ((units_sold * sale_price) - ((units_sold * sale_price) * discount_percentage)) AS sales,
                        units_sold *  manufacturing_price AS cogs,
                        ((units_sold * sale_price) - ((units_sold * sale_price) * discount_percentage)) - units_sold *  manufacturing_price AS profit
                    FROM 
                        `{BQ_DATASET}.{BQ_TABLE}`;
                    """,
            "useLegacySql": False,
        },
        gcp_conn_id=GCP_CONN_ID  # Use GCP connection ID in Airflow
    )

postgres_to_gcs >> bq_load_csv >> bq_create_calculate_view