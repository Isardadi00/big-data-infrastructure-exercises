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

        print(f"Retrieved {file} from s3 bucket {s3_bucket}")
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

