import concurrent.futures
import os
from typing import Annotated

import duckdb as db
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import ujson
from bs4 import BeautifulSoup
from fastapi import APIRouter, status
from fastapi.params import Query

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

    if os.path.exists(download_dir):
        for file in os.listdir(download_dir):
            os.remove(os.path.join(download_dir, file))
    os.makedirs(download_dir, exist_ok=True)

    response = requests.get(base_url)

    soup = BeautifulSoup(response.text, "html.parser")

    file_links = [a['href'] for a in soup.find_all("a") if a["href"].endswith(".json.gz")][:file_limit]

    def download_file(file):
        response = requests.get(base_url + file)
        with open(os.path.join(download_dir, file[:-3]), "wb") as f:
            f.write(response.content)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(download_file, file_links)


    return "OK"

# multiprocessing can only serialize top-module level functions which
# requires the function to be 'pickleable' so it can be passed to the child processes
# this is why the function is defined outside the route
def prepare_file(file):
        file_path = os.path.join(settings.raw_dir, "day=20231101", file)
        with open(file_path) as f:
            data = ujson.load(f)

        if 'aircraft' in data:
            timestamp = data['now']
            df = pd.DataFrame(data['aircraft'])
            df_new = pd.DataFrame()
            emergency_values = ['general', 'lifeguard', 'minfuel', 'nordo', 'unlawful', 'downed', 'reserved']
            df_new['altitude_baro'] = df['alt_baro'].replace({'ground': 0})
            df_new['had_emergency'] = df['emergency'].isin(emergency_values)

            df_new['icao'] = df.get('hex', None)
            df_new['registration'] = df.get('r', None)
            df_new['type'] = df.get('t', None)
            df_new['lat'] = df.get('lat', None)
            df_new['lon'] = df.get('lon', None)
            df_new['ground_speed'] = df.get('gs', None)
            df_new['timestamp'] = timestamp


        else:
            print(f"File {file} does not have aircraft data")

        return df_new


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


    if os.path.exists(prepared_dir):
        for file in os.listdir(prepared_dir):
            os.remove(os.path.join(prepared_dir, file))
    os.makedirs(prepared_dir, exist_ok=True)

    files = os.listdir(raw_dir)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = executor.map(prepare_file, files)

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
