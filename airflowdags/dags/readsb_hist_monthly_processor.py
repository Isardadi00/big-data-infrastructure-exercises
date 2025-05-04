import gzip
import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from psycopg_pool import ConnectionPool

S3_BUCKET = os.getenv("S3_BUCKET")
S3_URL = os.getenv("S3_URL")
SOURCE_URL = "https://samples.adsbexchange.com/readsb-hist"

logger = logging.getLogger(__name__)
s3 = boto3.client("s3")

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

def download_data(ds, **_):
    if not S3_BUCKET:
        logger.error("S3_BUCKET environment variable is not set.")
        raise ValueError("S3_BUCKET environment variable is not set.")

    date_str_url = datetime.strptime(ds, "%Y-%m-%d").strftime("/%Y/%m/%d/") # Format for URL
    date_str_s3 = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y%m%d")  # Format for S3 prefix
    base_url = SOURCE_URL + date_str_url
    logger.info(f"Downloading from: {base_url}")
    s3_bucket = S3_BUCKET
    s3_prefix = f"raw/{date_str_s3}"

    file_limit = 100  # Limit the number of files to download

    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    file_links = [a['href'] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]
    logger.info(f"Found {len(file_links)} files to download.")

    s3 = boto3.client('s3')

    # Check for existing files in S3
    existing = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    # If there are already 100+ files, skip downloading
    if existing.get("KeyCount", 0) >= 100:
        logger.info(f"{s3_prefix} already has >= 100 files. Skipping download.")
        return

    for file in file_links:
        file_url = f"{base_url}/{file}"
        s3_key = f"{s3_prefix}/{file}"

        try:
            response = requests.get(file_url, stream=True, timeout=10)
            if response.status_code == 200:
                s3.upload_fileobj(response.raw, s3_bucket, s3_key)
                print(f"Uploaded: {s3_key}")
            else:
                print(f"Failed to download {file_url}")
        except Exception as e:
            print(f"Error downloading {file_url}: {e}")


def prepare_data(ds, **_):
    date_s3 = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y%m%d")
    s3_bucket = S3_BUCKET
    s3_prefix_raw = f"raw/{date_s3}"
    s3_prefix_prepared = f"prepared/{date_s3}/"

    # Create local directory for the day to store files in memory
    local_day_dir = os.path.join("/usr/local/airflow/data", f"day={date_s3}")
    os.makedirs(local_day_dir, exist_ok=True)

    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_raw)
    for obj in response.get("Contents", []):
        s3_key = obj["Key"]
        filename = os.path.basename(s3_key).replace(".gz", "")
        local_file = os.path.join(local_day_dir, filename)

        if os.path.exists(local_file):
            logger.info(f"{filename} already processed. Skipping.")
            continue

        try:
            s3_response = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
            with gzip.GzipFile(fileobj=io.BytesIO(s3_response["Body"].read())) as gz_file:
                data = json.loads(gz_file.read().decode("utf-8"))

            timestamp = data["now"]
            aircraft_data = data.get("aircraft", [])

            processed = [
                {
                    "icao": ac.get("hex"),
                    "registration": ac.get("r"),
                    "timestamp": timestamp,
                }
                for ac in aircraft_data
            ]

            processed_json = json.dumps(processed)
            s3_key_prepared = s3_prefix_prepared + os.path.basename(s3_key).replace(".gz", "")
            s3.put_object(Body=processed_json, Bucket=s3_bucket, Key=s3_key_prepared)

            logger.info(f"Prepared and uploaded: {s3_key_prepared}")

        except Exception as e:
            logger.error(f"Error processing {s3_key}: {e}")


def insert_aircraft_data_to_RDS(ds, **_):
    date_s3 = datetime.strptime(ds, "%Y-%m-%d").strftime("%Y/%m/%d")
    s3_bucket = S3_BUCKET
    s3_prefix = f"prepared/{date_s3}/"

    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    logger.info(f"Found {len(response.get('Contents', []))} files to process.")
    for obj in response.get("Contents", []):
        s3_key = obj["Key"]

        obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        with obj["Body"] as file_stream:
            json_data = json.loads(file_stream.read())

        logger.info(f"Processing {s3_key} with {len(json_data)} records for data from S3.")
        timestamp = json_data['now']
        df = pd.DataFrame(json_data['aircraft'])
        df_new = pd.DataFrame()
        df_new['icao'] = df.get('hex', None)
        df_new['registration'] = df.get('r', None)
        df_new['type'] = df.get('t', None)
        df_new['timestamp'] = timestamp

        logger.info(f"Processing {s3_key} with {len(df_new)} records for data in dataframe.")
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO aircraft (icao, registration, type, timestamp)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.executemany(insert_query, df_new.values.tolist())
                conn.commit()

with DAG(
    dag_id="readsb_hist_monthly_processor",
    description="Download and prepare readsb-hist data on the 1st of each month",
    schedule_interval="@monthly",  # runs only on the 1st of each month
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 2),  # inclusive of 2024-11-01
    catchup=True,
    tags=["readsb", "aircraft"],
) as dag:

    download = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
        dag=dag,
    )

    prepare = PythonOperator(
        task_id="prepare_data",
        python_callable=prepare_data,
        dag=dag,
    )

    insert_to_rds = PythonOperator(
        task_id="insert_aircraft_data_to_RDS",
        python_callable=insert_aircraft_data_to_RDS,
        dag=dag,
    )

    download >> prepare >> insert_to_rds
