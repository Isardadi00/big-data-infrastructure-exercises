import psycopg
import boto3
import json
from bdi_api.s7.s7_funcs import S7
from fastapi import APIRouter, status
from bdi_api.settings import DBCredentials, Settings

settings = Settings()
db_credentials = DBCredentials()
BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """
    
    s3 = boto3.client('s3')
    bucket_name = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    conn = psycopg.connect(
        dbname="postgres", 
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port,
    )

    cursor = conn.cursor()

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix_path)
    files = [content['Key'] for content in response.get('Contents')]

    create_table_query = """
        CREATE TABLE IF NOT EXISTS aircraft (
            icao VARCHAR(7),
            registration VARCHAR(10),
            type VARCHAR(10),
            lat FLOAT,
            lon FLOAT,
            ground_speed FLOAT,
            altitude_baro FLOAT,
            timestamp FLOAT,
            had_emergency BOOLEAN
        ) 
        """
    cursor.execute(create_table_query)

    for file in files:
        json_data = S7.retrieve_from_s3_bucket(s3, bucket_name, file)
        df = S7.prepare_file(json_data)
        df = df[['icao', 'registration', 'type', 'lat', 'lon', 'ground_speed', 'altitude_baro', 'timestamp', 'had_emergency']]

        print(df.columns)
        print(df.head())

        insert_query = """
            INSERT INTO aircraft (icao, registration, type, lat, lon, ground_speed, altitude_baro, timestamp, had_emergency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()

    cursor.close()
    conn.close()

    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = psycopg.connect(
        dbname="postgres", 
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port,
    )

    cursor = conn.cursor()

    cursor.execute(
        f"""SELECT DISTINCT icao,
            registration,
            type FROM aircraft
            ORDER BY icao ASC LIMIT {num_results}
            OFFSET {page * num_results}"""
    )
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in result]


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = psycopg.connect(
        dbname="postgres", 
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port,
    )

    cursor = conn.cursor()

    cursor.execute(
        f"""SELECT timestamp,
            lat,
            lon FROM aircraft
            WHERE icao = '{icao}'
            AND lat IS NOT NULL
            AND lon IS NOT NULL
            ORDER BY timestamp ASC LIMIT {num_results}
            OFFSET {page * num_results}"""
    )

    result = cursor.fetchall()
    cursor.close()
    conn.close()

    return [{"timestamp": row[0], "lat": row[1], "lon": row[2]} for row in result]


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft

    * max_altitude_baro
    * max_ground_speed  
    * had_emergency

    FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """

    conn = psycopg.connect(
        dbname="postgres",
        user=db_credentials.username,
        password=db_credentials.password,
        host=db_credentials.host,
        port=db_credentials.port,
    )
    cursor = conn.cursor()

    cursor.execute(
        f"""SELECT MAX(altitude_baro) as max_altitude_baro,
            MAX(ground_speed) as max_ground_speed,
            BOOL_OR(had_emergency) as had_emergency FROM aircraft
            WHERE icao = '{icao}'
            """
    )
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result is None:
        return {}
    return {
        "max_altitude_baro": result[0],
        "max_ground_speed": result[1],
        "had_emergency": result[2],
    }