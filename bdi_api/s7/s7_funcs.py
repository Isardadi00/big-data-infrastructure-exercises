import pandas as pd
import ujson

from bdi_api.settings import Settings

settings = Settings()

pd.set_option('future.no_silent_downcasting', True)

class S7:
    def retrieve_from_s3_bucket(s3, s3_bucket, file):
        obj = s3.get_object(Bucket=s3_bucket, Key=file)

        with obj['Body'] as file_stream:
            json_data = ujson.loads(file_stream.read())

        return json_data

    def prepare_file(file):
            if 'aircraft' in file:
                timestamp = file['now']
                df = pd.DataFrame(file['aircraft'])
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
    
    def insert_data_into_database(s3, bucket_name, pool, file):
        json_data = S7.retrieve_from_s3_bucket(s3, bucket_name, file)
        df = S7.prepare_file(json_data)
        df = df[['icao', 'registration', 'type', 'lat', 'lon', 'ground_speed', 'altitude_baro', 'timestamp', 'had_emergency']]

        insert_query = """
                INSERT INTO aircraft (icao, registration, type, lat, lon, ground_speed, altitude_baro, timestamp, had_emergency)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_query, df.values.tolist())
                conn.commit()


    def create_aircraft_table(pool):
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
                conn.commit()

    def create_aircraft_stats_view(pool):
        with pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE MATERIALIZED VIEW IF NOT EXISTS aircraft_stats AS
                        SELECT icao,
                        MAX(altitude_baro) AS max_altitude_baro,
                        MAX(ground_speed) AS max_ground_speed,
                        BOOL_OR(had_emergency) AS had_emergency
                        FROM aircraft
                        GROUP BY icao
                """)
                conn.commit()

                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_aircraft_icao ON aircraft_stats (icao);
                """)
                conn.commit()

