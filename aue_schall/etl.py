import pandas as pd
import os
import ftplib
import common
import urllib3
from datetime import datetime, timedelta
from aue_schall import credentials

today_string = datetime.today().strftime('%Y%m%d')
yesterday_string = datetime.strftime(datetime.today() - timedelta(1), '%Y%m%d')
local_files = {}
stations = []


# Retry with some delay in between if any explicitly defined error is raised
@common.retry(common.ftp_errors_to_handle, tries=6, delay=10, backoff=1)
def download_data_files():
    global date_string, station
    ftp = ftplib.FTP(credentials.ftp_read_server, credentials.ftp_read_user, credentials.ftp_read_pass)
    print(f'Changing to remote dir {credentials.ftp_read_remote_path}...')
    ftp.cwd(credentials.ftp_read_remote_path)
    print('Retrieving list of files...')
    for file_name, facts in ftp.mlsd():
        # If we only use today's date we might lose some values just before midnight yesterday.
        for date_string in [yesterday_string, today_string]:
            if date_string in file_name and 'OGD' in file_name:
                print(f"File {file_name} has 'OGD' and '{date_string}' in its filename. "
                      f'Parsing station name from filename...')
                station = file_name \
                    .replace(f'_{date_string}.csv', '') \
                    .replace('airmet_auebs_', '') \
                    .replace('_OGD', '')
                stations.append(station)
                print(f'Downloading {file_name} for station {station}...')
                local_file = os.path.join(credentials.path, file_name)
                with open(local_file, 'wb') as f:
                    ftp.retrbinary(f"RETR {file_name}", f.write)
                local_files[(station, date_string)] = local_file
    ftp.quit()


print(f'Connecting to FTP Server to read data...')
download_data_files()
dfs = {}
all_data = pd.DataFrame(columns=['LocalDateTime', 'Value', 'Latitude', 'Longitude', 'EUI'])
print('Reading csv files into data frames...')
urllib3.disable_warnings()
for station in stations:
    print(f'Retrieving latest timestamp for station "{station}" from ODS...')
    r = common.requests_get(url=f'https://data.bs.ch/api/records/1.0/search/?dataset=100087&q=&rows=1&sort=timestamp&refine.station_id={station}', verify=False)
    r.raise_for_status()
    latest_ods_timestamp = r.json()['records'][0]['fields']['timestamp']
    print(f'Latest timestamp is {latest_ods_timestamp}.')
    for date_string in [yesterday_string, today_string]:
        try:
            print(f"Reading {local_files[(station, date_string)]}...")
            df = pd.read_csv(local_files[(station, date_string)], sep=';', na_filter=False)
            print(f'Calculating ISO8601 time string...')
            df['timestamp'] = pd.to_datetime(df.LocalDateTime, format='%d.%m.%Y %H:%M',
                                             errors='coerce').dt.tz_localize('Europe/Zurich', ambiguous='infer')
            # Handle bad cases 14.09.2023
            is_invalid_hour = df['timestamp'].dt.hour == 24
            df.loc[is_invalid_hour, 'timestamp'] -= pd.DateOffset(hours=24)

            df.set_index('timestamp', drop=False, inplace=True)
            df['station_id'] = station
            all_data = all_data.append(df, sort=True)
            dfs[(station, date_string)] = df

            print(f'Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...')
            realtime_df = df[df['timestamp'] > latest_ods_timestamp]

            # Realtime API bootstrap data:
            # {
            #     "eui": "0004A30B00F156E2",
            #     "timestamp": "2021-02-22T09:51:00+00:00",
            #     "value": 24.1,
            #     "longitude": 7.594749,
            #     "latitude": 47.567005,
            #     "station_id": "Feldbergstrasse"
            # }

            print(f'Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...')
            for index, row in realtime_df.iterrows():
                timestamp_text = row.timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')
                payload = {'eui': row.EUI, 'timestamp': timestamp_text, 'value': row.Value, 'longitude': row.Longitude, 'latitude': row.Latitude, 'station_id': row.station_id}
                print(f'Pushing row {index} with with the following data to ODS: {payload}')
                r = common.requests_post(url=credentials.ods_live_push_api_url, json=payload, verify=False)
                r.raise_for_status()
        except KeyError as e:
            print(f'No file found with keys {(station, date_string)}, ignoring...')

all_data = all_data[['station_id', 'timestamp', 'Value', 'Latitude', 'Longitude', 'EUI', 'LocalDateTime']]
today_data_file = os.path.join(credentials.path, f'schall_aktuell.csv')
print(f"Exporting yesterday's and today's data to {today_data_file}...")
all_data.to_csv(today_data_file, index=False)

# todo: Simplify code by pushing yesterday's and today's data to ODS in one batch (as in lufthygiene_pm25)

print('Creating stations file from current data file...')
df_stations = all_data.drop_duplicates(['EUI'])[['station_id', 'Latitude', 'Longitude', 'EUI']]
stations_file = os.path.join(credentials.path, 'stations/stations.csv')
print(f'Exporting stations file to {stations_file}...')
df_stations.to_csv(stations_file, index=False)

common.upload_ftp(stations_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'{credentials.ftp_remote_path_stations}')
common.upload_ftp(today_data_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_path_vals)

print('Job successful!')
