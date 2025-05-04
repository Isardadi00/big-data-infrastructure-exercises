from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import boto3
import requests
from botocore.exceptions import ClientError

# Constants
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY_FUEL_CONSUMPTION = os.getenv("S3_KEY_FUEL_CONSUMPTION")
SOURCE_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"

s3 = boto3.client("s3")

def download_and_upload_to_s3_if_missing():
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY_FUEL_CONSUMPTION)
        return  # File already exists, no need to download
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise  # If the error isn't 'Not Found', re-raise

    response = requests.get(SOURCE_URL, timeout=10)
    if response.status_code == 200:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=S3_KEY_FUEL_CONSUMPTION,
            Body=response.text.encode("utf-8"),
            ContentType="application/json"
        )
    else:
        raise Exception(f"Download failed with status code: {response.status_code}")

with DAG(
    dag_id="download_and_upload_fuel_consumption_data_to_s3",
    start_date=datetime(2023, 11, 1),
    schedule_interval='@once',
    catchup=True,
    description="Download aircraft fuel consumption data and upload to S3 if not already there",
    tags=["reference", "aircraft"],
) as dag:

    upload_to_s3 = PythonOperator(
        task_id="download_and_upload_fuel_data",
        python_callable=download_and_upload_to_s3_if_missing,
        dag=dag,
    )

    upload_to_s3