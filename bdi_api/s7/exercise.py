import functools
import boto3
import concurrent.futures
from psycopg_pool import ConnectionPool
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

pool = ConnectionPool(
    conninfo=f"""
        dbname=postgres
        user={db_credentials.username}
        password={db_credentials.password}
        host={db_credentials.host}
        port={db_credentials.port}
    """,
    max_size=20,
    max_lifetime=600,
    timeout=10
)

@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS

    Use credentials passed from `db_credentials`
    """
    
    s3 = boto3.client('s3')
    bucket_name = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix_path)
    files = [content['Key'] for content in response.get('Contents')]

    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
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
            )

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_aircraft_icao ON aircraft (icao);
            """)

            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(
                functools.partial(S7.insert_data_into_database, s3, bucket_name, conn, cursor),
                files
            )

    return "OK"


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    
    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""SELECT DISTINCT icao,
                    registration,
                    type FROM aircraft
                    ORDER BY icao ASC LIMIT {num_results}
                    OFFSET {page * num_results}"""
            )
            result = cursor.fetchall()

    return [{"icao": row[0], "registration": row[1], "type": row[2]} for row in result]


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    
    with pool.connection() as conn:
        with conn.cursor() as cursor:
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

    with pool.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT MAX(altitude_baro) AS max_altitude_baro,
                    MAX(ground_speed) AS max_ground_speed,
                    BOOL_OR(had_emergency) AS had_emergency
                FROM aircraft
                WHERE icao = %s
            """, (icao,))
            result = cursor.fetchone()

    if result.count(None) == 3:
        return {}
    return {
        "max_altitude_baro": result[0],
        "max_ground_speed": result[1],
        "had_emergency": result[2],
    }