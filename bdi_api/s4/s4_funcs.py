import concurrent
import io
import os
from functools import partial

import pandas as pd
import requests
import ujson

from bdi_api.settings import Settings

settings = Settings()

pd.set_option('future.no_silent_downcasting', True)

class S4:

    def remove_and_create_dir(dir):
        if os.path.exists(dir):
            for file in os.listdir(dir):
                os.remove(os.path.join(dir, file))
        os.makedirs(dir, exist_ok=True)

    def check_if_bucket_exists(s3_client, bucket_name):
        return any(bucket['Name'] == bucket_name for bucket in s3_client.list_buckets().get('Buckets', []))

    def download_to_s3_bucket(base_url, s3, s3_bucket, s3_prefix_path, file):
        response = requests.get(base_url + file)
        if response.status_code == 200:
            file_obj = io.BytesIO(response.content)
            s3.upload_fileobj(file_obj, s3_bucket, s3_prefix_path + file[:-3])

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

    def process_files_in_batches_stream(files, s3, s3_bucket, batch_size=100, max_workers=10):
        retrieve_from_s3_bucket_partial = partial(S4.retrieve_from_s3_bucket, s3, s3_bucket)

        for i in range(0, len(files), batch_size):
            batch_files = files[i:i + batch_size]
            print(f"Processing batch {i//batch_size + 1} with {len(batch_files)} files.")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(retrieve_from_s3_bucket_partial, batch_files))

            yield from results

