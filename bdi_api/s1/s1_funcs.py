import os

import pandas as pd
import requests
import ujson

from bdi_api.settings import Settings

settings = Settings()

class S1:
    def download_file(base_url, download_dir, file):
                response = requests.get(base_url + file)
                with open(os.path.join(download_dir, file[:-3]), "wb") as f:
                    f.write(response.content)

    def remove_and_create_dir(dir):
        if os.path.exists(dir):
            for file in os.listdir(dir):
                os.remove(os.path.join(dir, file))
        os.makedirs(dir, exist_ok=True)

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
