from ftplib import FTP
import os
import pandas as pd
import common
from bafu_hydrodaten import credentials

print(f'Connecting to FTP Server to read data...')
ftp = FTP(credentials.ftp_read_server, credentials.ftp_read_user, credentials.ftp_read_pass)
print(f'Changing to remote dir {credentials.ftp_read_remote_path}...')
ftp.cwd(credentials.ftp_read_remote_path)
local_path = 'bafu_hydrodaten/data'
files = [credentials.abfluss_file, credentials.pegel_file]
local_files = []
for file in files:
    local_file = os.path.join(local_path, file)
    local_files.append(local_file)
    print(f'Retrieving file {local_file}...')
    with open(local_file, 'wb') as f:
        ftp.retrbinary(f"RETR {file}", f.write)
ftp.quit()

print('Loading and processing data...')
abfluss_df = pd.read_csv(local_files[0], sep='\t', skiprows=4, names=['datum', 'zeit', 'abfluss', 'intervall', 'qualitaet', 'messart'], header=None)
pegel_df = pd.read_csv(local_files[1], sep='\t', skiprows=4, names=['datum', 'zeit', 'pegel', 'intervall', 'qualitaet', 'messart'], header=None)

abfluss_df = abfluss_df[abfluss_df.intervall == 5]
print('Replacing 24:00 with 23:59...')
abfluss_df.loc[abfluss_df.zeit == '24:00', 'zeit'] = '23:59'
abfluss_df['timestamp'] = pd.to_datetime(abfluss_df.datum + ' ' + abfluss_df.zeit, format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Zurich')
print('Adding a minute to entries with time 23:59...')
# todo: Add a minute to entries with zeit == 23:59
abfluss_df.to_csv(os.path.join(local_path, 'abfluss.csv'), index=False)

# Merge into a single df, then export to csv

# for local_file in local_files:
#     common.upload_ftp(local_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_dir)
# print('Job successful!')
