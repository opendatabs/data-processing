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
        print('Parsing station name from filename...')
        station = file_name.replace(f'_{date_string}.csv', '').replace('airmet_auebs_', '')
        stations.append(station)

        print(f'Downloading {file_name}...')
        local_file = os.path.join(credentials.path, file_name)
        with open(local_file, 'wb') as f:
            ftp.retrbinary(f"RETR {file_name}", f.write)

        local_files[station] = local_file
        # print(facts);

dfs = {}
print('Reading csv files into data frames...')
for station in stations:
    print(f"Reading {local_files[station]}...")
    df = pd.read_csv(local_files[station], sep=';', skiprows=1, na_filter=False)
    df.columns = ['timestamp_text', 'dB']
    df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Zurich')
    df.set_index('timestamp', drop=False, inplace=True)
    df = df[['timestamp', 'dB', 'timestamp_text']]
    dfs[station] = df


# files = [credentials.abfluss_file, credentials.pegel_file]
# local_files = []
# for file in files:
#     local_file = f'bafu_hydrodaten/data/{file}'
#     local_files.append(local_file)
#     print(f'Retrieving file {local_file}...')
#     with open(local_file, 'wb') as f:
#         ftp.retrbinary(f"RETR {file}", f.write)
ftp.quit()

