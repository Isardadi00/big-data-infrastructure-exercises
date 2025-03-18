import concurrent
import os
from typing import Annotated

import boto3
import boto3.session
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.s4.s4_funcs import S4
from bdi_api.settings import Settings

settings = Settings()


s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
    Limits the number of files to download.
    You must always start from the first the page returns and
    go in ascending order in order to correctly obtain the results.
    I'll test with increasing number of files starting from 100.""",
        ),
    ] = 100,
) -> str:
    """Same as s1 but store to an aws s3 bucket taken from settings
    and inside the path `raw/day=20231101/`

    NOTE: you can change that value via the environment variable `BDI_S3_BUCKET`
    """

    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")
    file_links = [a['href'] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]

    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')

    print(f"Downloading {len(file_links)} files to s3 bucket {s3_bucket} at path {s3_prefix_path}")

    if S4.check_if_bucket_exists(s3_client, s3_bucket) is False:
        s3_client.create_bucket(Bucket=s3_bucket)
    else: 
        print(f"Bucket {s3_bucket} already exists")
        S4.delete_from_s3_bucket(s3_resource, s3_bucket)
        print(f"Deleting all files in bucket {s3_bucket}")


    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(lambda file: S4.download_to_s3_bucket(base_url, s3_client, s3_bucket, s3_prefix_path, file), file_links)

    return "OK"


@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s2.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    S4.remove_and_create_dir(prepared_dir)

    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix_path)
    s3_files = [obj['Key'] for obj in response['Contents']]
    print(f"Found {len(s3_files)} files in s3 bucket {s3_bucket}")



    s3_results = S4.process_files_in_batches_stream(s3_files, s3, s3_bucket, batch_size=100, max_workers=10)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(S4.prepare_file, s3_results)

        tables = [pa.Table.from_pandas(result) for result in results]
        schema = tables[0].schema
        tables = [table.cast(schema) for table in tables]
        table = pa.concat_tables(tables)
        pq.write_table(table, prepare_file_path)

    return "OK"
