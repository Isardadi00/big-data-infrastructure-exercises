import concurrent.futures
import os
from typing import Annotated

import duckdb as db
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.s1.s1_funcs import S1
from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s1",
    tags=["s1"],
)

pd.set_option('future.no_silent_downcasting', True)

@s1.post("/aircraft/download")
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
    """Downloads the `file_limit` files AS IS inside the folder data/20231101

    data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.


    TIP: always clean the download folder before writing again to avoid having old files.
    """
    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    S1.remove_and_create_dir(download_dir)
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, "html.parser")

    file_links = [a['href'] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(lambda file: S1.download_file(base_url, download_dir, file), file_links)

    return "OK"


@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """Prepare the data in the way you think it's better for the analysis.

    * data: https://samples.adsbexchange.com/readsb-hist/2023/11/01/
    * documentation: https://www.adsbexchange.com/version-2-api-wip/
        See "Trace File Fields" section

    Think about the way you organize the information inside the folder
    and the level of preprocessing you might need.

    To manipulate the data use any library you feel comfortable with.
    Just make sure to configure it in the `pyproject.toml` file
    so it can be installed using `poetry update`.

    TIP: always clean the prepared folder before writing again to avoid having old files.

    Keep in mind that we are downloading a lot of small files, and some libraries might not work well with this!
    """

    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = os.path.join(prepared_dir, 'aircraft_data.parquet')

    S1.remove_and_create_dir(prepared_dir)

    files = os.listdir(raw_dir)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(S1.prepare_file, files)

        tables = [pa.Table.from_pandas(result) for result in results]
        schema = tables[0].schema
        tables = [table.cast(schema) for table in tables]
        table = pa.concat_tables(tables)
        pq.write_table(table, prepare_file_path)

    return "OK"


@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = 'aircraft_data.parquet'

    result = db.query(
        f"""SELECT DISTINCT icao,
            registration,
            type FROM '{os.path.join(prepared_dir, prepare_file_path)}'
            ORDER BY icao ASC LIMIT {num_results}
            OFFSET {page * num_results}"""
    ).df()

    return result.to_dict(orient='records')

    #return [{"icao": "0d8300", "registration": "YV3382", "type": "LJ31"}]


@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list.
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = 'aircraft_data.parquet'

    result = db.query(
        f"""SELECT timestamp,
            lat,
            lon FROM '{os.path.join(prepared_dir, prepare_file_path)}'
            WHERE icao = '{icao}'
            AND lat IS NOT NULL
            AND lon IS NOT NULL
            ORDER BY timestamp ASC LIMIT {num_results}
            OFFSET {page * num_results}"""
    ).df()

    return result.to_dict(orient='records')

    #return [{"timestamp": 1609275898.6, "lat": 30.404617, "lon": -86.476566}]


@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed
    * had_emergency
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    prepare_file_path = 'aircraft_data.parquet'

    result =db.query(
        f"""SELECT MAX(altitude_baro) as max_altitude_baro,
            MAX(ground_speed) as max_ground_speed,
            MAX(had_emergency) as had_emergency FROM '{os.path.join(prepared_dir, prepare_file_path)}'
            WHERE icao = '{icao}'
            """
    ).df()
    return result.to_dict(orient='records')[0]

    #return {"max_altitude_baro": 300000, "max_ground_speed": 493, "had_emergency": False}
