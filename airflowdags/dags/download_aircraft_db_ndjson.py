import gzip
import json
import logging
import os
from datetime import datetime

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg_pool import ConnectionPool

# Config
logger = logging.getLogger(__name__)
SOURCE_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
S3_BUCKET = os.getenv("S3_BUCKET")

db_credentials = {
    "username": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

pool = ConnectionPool(
    conninfo=f"""
        dbname=postgres
        user={db_credentials['username']}
        password={db_credentials['password']}
        host={db_credentials['host']}
        port={str(db_credentials['port'])}
    """,
    max_size=20,
    max_lifetime=600,
    timeout=20
)

def upload_aircraft_data_to_s3():
    if not S3_BUCKET:
        logger.error("S3_BUCKET environment variable is not set.")
        raise ValueError("S3_BUCKET environment variable is not set.")

    s3_prefix = "basic-ac-data/"
    logger.info(f"Downloading from: {SOURCE_URL}")

    s3_client = boto3.client("s3")

    # Remove existing files in S3 bucket prefix
    try:
        existing_files = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=s3_prefix)
        if existing_files.get("Contents"):
            for obj in existing_files["Contents"]:
                s3_client.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
                logger.info(f"Deleted: {obj['Key']}")
    except Exception as e:
        logger.error(f"Error deleting existing files: {e}")
        raise

    try:
        # download the file in stream and upload as .json to s3
        response = requests.get(SOURCE_URL, stream=True)
        if response.status_code != 200:
            logger.error(f"Failed to download from {SOURCE_URL}, status code: {response.status_code}")
            raise Exception(f"Failed to download from {SOURCE_URL}, status code: {response.status_code}")
        logger.info("Download successful, processing data...")

        # Decompress the gzip file
        decompressed = gzip.decompress(response.content).decode("utf-8")
        # Convert from NDJSON to JSON to reduce file size
        data = [json.loads(line) for line in decompressed.strip().split("\n")]
        logger.info("Data processing complete, uploading to S3...")

        # Save the JSON data directly to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_prefix + "basic-ac-db.json",
            Body=json.dumps(data),
            ContentType="application/json"
        )

        logger.info(f"Uploaded to S3: {s3_prefix}basic-ac-db.json")
        logger.info(f"Total records: {len(data)}")


    except Exception as e:
        logger.exception(f"Failed to download from {SOURCE_URL}, status code: {response.status_code}, error: {e}")
        raise

def insert_aircraft_data_to_RDS():
    logger.info(f"DB credentials: {db_credentials}")
    logger.info(f"S3 bucket: {S3_BUCKET}")

    s3_bucket = S3_BUCKET
    s3_prefix = "basic-ac-data/basic-ac-db.json"
    s3_href = f"s3://{s3_bucket}/{s3_prefix}"
    logger.info(f"Inserting into RDS with data from: {s3_href}")

    # Load the JSON data from S3
    s3_client = boto3.client("s3")

    try :
        response = s3_client.get_object(Bucket=S3_BUCKET, Key="basic-ac-data/basic-ac-db.json")
    except Exception as e:
        logger.error(f"Error downloading {s3_href}: {e}")
        return

    data = json.loads(response["Body"].read().decode("utf-8"))

    logger.info(f"Loaded {len(data)} records from S3")
    try:
        logger.info("Inserting data into PostgreSQL database...")
        if not data:
            logger.warning("No data found in the S3 object.")
            return
        rows = [
            (
                aircraft.get("icao"),
                aircraft.get("reg"),
                aircraft.get("icaotype"),
                aircraft.get("ownop"),
                aircraft.get("manufacturer"),
                aircraft.get("model"),
            )
            for aircraft in data
        ]
        logger.info(f"First record for debugging: {rows[0]}")

        with pool.connection() as conn:
            with conn.cursor() as cursor:
                create_table_query = """
                    CREATE TABLE IF NOT EXISTS aircraft_data (
                        icao VARCHAR(6) PRIMARY KEY,
                        registration TEXT,
                        icaotype TEXT,
                        ownop TEXT,
                        manufacturer TEXT,
                        model TEXT
                    )
                """
                cursor.execute(create_table_query)
                conn.commit()

                insert_query = """
                    INSERT INTO aircraft_data (icao, registration, icaotype, ownop, manufacturer, model)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (icao) DO NOTHING
                """
                cursor.executemany(insert_query, rows)
                conn.commit()

        logger.info(f"Inserted {len(rows)} records into the database")

    except Exception as e:
        logger.error(f"Error uploading data from S3 to RDS: {e}")
        return



with DAG(
    dag_id="download_aircraft_db_ndjson",
    start_date=datetime(2025, 4, 15),
    schedule_interval="@once",
    catchup=True,
    description="Download and extract the aircraft database from ADS-B Exchange (NDJSON)",
    tags=["aircraft", "database"],
) as dag:

    upload = PythonOperator(
        task_id="upload_aircraft_data_to_s3",
        python_callable=upload_aircraft_data_to_s3,
        dag=dag,

    )

    insert = PythonOperator(
        task_id="insert_aircraft_data_to_RDS",
        python_callable=insert_aircraft_data_to_RDS,
        dag=dag,
    )

    upload >> insert
