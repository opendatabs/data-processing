import pandas as pd
import os
from ftplib import FTP
import common
from datetime import datetime
from aue_schall import credentials

date_string = datetime.today().strftime('%Y%m%d')

local_files = {}
stations = []

print(f'Connecting to FTP Server to read data...')
ftp = FTP(credentials.ftp_read_server, credentials.ftp_read_user, credentials.ftp_read_pass)
print(f'Changing to remote dir {credentials.ftp_read_remote_path}...')
ftp.cwd(credentials.ftp_read_remote_path)
print('Retrieving list of files...')
for file_name, facts in ftp.mlsd():
    if date_string in file_name:
        print(f'File {file_name} has current date ({date_string}) in its filename. Parsing station name from filename...')
        station = file_name.replace(f'_{date_string}.csv', '').replace('airmet_auebs_', '')
        stations.append(station)

        print(f'Downloading {file_name} for station {station}...')
        local_file = os.path.join(credentials.path, file_name)
        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {file_name}", f.write)

        local_files[station] = local_file
ftp.quit()

dfs = {}
all_data = pd.DataFrame(columns=['station_id', 'timestamp', 'dB'])
print('Reading csv files into data frames...')
for station in stations:
    print(f"Reading {local_files[station]}...")
    df = pd.read_csv(local_files[station], sep=';', skiprows=1, na_filter=False)
    df.columns = ['timestamp_text', 'dB']
    print(f'Calculating ISO8601 time string...')
    df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Zurich')
    df.set_index('timestamp', drop=False, inplace=True)
    df = df[['timestamp', 'dB']]
    df['station_id'] = station
    all_data = all_data.append(df, sort=True)
    dfs[station] = df

all_data = all_data[['station_id', 'timestamp', 'dB']]
today_data_file = os.path.join(credentials.path, f'schall_{date_string}.csv')
print(f"Exporting today's data to {today_data_file}...")
all_data.to_csv(today_data_file, index=False)

common.upload_ftp(today_data_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_path)
print('Job successful!')