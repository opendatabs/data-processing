import pandas as pd
import os
import ftplib
import common
from datetime import datetime, timedelta
from aue_schall import credentials

today_string = datetime.today().strftime('%Y%m%d')
yesterday_string = datetime.strftime(datetime.today() - timedelta(1), '%Y%m%d')
local_files = {}
stations = []


# Retry with some delay in between if any explicitly defined error is raised
@common.retry(common.ftp_errors_to_handle(), tries=6, delay=10, backoff=1)
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
for station in stations:
    for date_string in [yesterday_string, today_string]:
        print(f"Reading {local_files[(station, date_string)]}...")
        df = pd.read_csv(local_files[(station, date_string)], sep=';', na_filter=False)
        print(f'Calculating ISO8601 time string...')
        df['timestamp'] = pd.to_datetime(df.LocalDateTime, format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Zurich', ambiguous='infer')
        df.set_index('timestamp', drop=False, inplace=True)
        df['station_id'] = station
        all_data = all_data.append(df, sort=True)
        dfs[(station, date_string)] = df

all_data = all_data[['station_id', 'timestamp', 'Value', 'Latitude', 'Longitude', 'EUI', 'LocalDateTime']]
today_data_file = os.path.join(credentials.path, f'schall_aktuell.csv')
print(f"Exporting yesterday's and today's data to {today_data_file}...")
all_data.to_csv(today_data_file, index=False)

print('Creating stations file from current data file...')
df_stations = all_data.drop_duplicates(['EUI'])[['station_id', 'Latitude', 'Longitude', 'EUI']]
stations_file = os.path.join(credentials.path, 'stations/stations.csv')
print(f'Exporting stations file to {stations_file}...')
df_stations.to_csv(stations_file, index=False)

common.upload_ftp(stations_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'{credentials.ftp_remote_path_stations}')
common.upload_ftp(today_data_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_path_vals)

print('Job successful!')
