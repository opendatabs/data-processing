from datetime import datetime
import numpy as np
import os
import pandas as pd
import common
from bafu_hydrodaten import credentials


print(f'Connecting to HTTPS Server to read data...')
local_path = os.path.join(credentials.path, 'bafu_hydrodaten/data')
files = [credentials.abfluss_file, credentials.pegel_file]
local_files = []
for file in files:
    local_file = os.path.join(local_path, file)
    local_files.append(local_file)
    print(f'Retrieving file {local_file}...')
    with open(local_file, 'wb') as f:
        uri = f'{credentials.https_url}/{file}'
        print(f'Reading data from {uri}...')
        # r = requests.get(url, auth=(credentials.https_user, credentials.https_pass))
        r = common.requests_get(url=uri, auth=(credentials.https_user, credentials.https_pass))
        f.write(r.content)


print('Loading data into data frames...')
abfluss_df = pd.read_csv(local_files[0], sep='\t', skiprows=4, names=['datum', 'zeit', 'abfluss', 'intervall', 'qualitaet', 'messart'], usecols=['datum', 'zeit', 'abfluss', 'intervall'], header=None)
pegel_df = pd.read_csv(local_files[1], sep='\t', skiprows=4, names=['datum', 'zeit', 'pegel', 'intervall', 'qualitaet', 'messart'], usecols=['datum', 'zeit', 'pegel', 'intervall'], header=None)

print(f'Merging data frames...')
merged_df = abfluss_df.merge(pegel_df, on=['datum', 'zeit', 'intervall'], how='outer')

print('Processing data...')
merged_df = merged_df.loc[merged_df.intervall == 5]
print(f'Fixing entries with zeit == 24:00...')
# Replacing 24:00 with 23:59
merged_df.loc[merged_df.zeit == '24:00', 'zeit'] = '23:59'
# Time is given in MEZ (UTC+1) thus use 'Etc/GMT-1' according to https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
# merged_df['timestamp'] = pd.to_datetime(merged_df.datum + ' ' + merged_df.zeit, format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Zurich')
merged_df['timestamp'] = pd.to_datetime(merged_df.datum + ' ' + merged_df.zeit, format='%d.%m.%Y %H:%M').dt.tz_localize('Etc/GMT-1')
# Adding a minute to entries with time 23:59 then replacing 23:59 with 24:00 again
merged_df.timestamp = np.where(merged_df.zeit != '23:59', merged_df.timestamp, merged_df.timestamp + pd.Timedelta(minutes=1))
merged_df.zeit = np.where(merged_df.zeit == '23:59', '24:00', merged_df.zeit)

merged_filename = os.path.join(local_path, f'2289_pegel_abfluss_{datetime.today().strftime("%Y-%m-%d")}.csv')
merged_df.to_csv(merged_filename, index=False)

common.upload_ftp(merged_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, credentials.ftp_remote_dir)

print(f'Retrieving latest record from ODS...')
# r = requests.get('https://data.bs.ch/api/records/1.0/search/?dataset=100089&q=&rows=1&sort=timestamp')
r = common.requests_get(url='https://data.bs.ch/api/records/1.0/search/?dataset=100089&q=&rows=1&sort=timestamp')
r.raise_for_status()
latest_ods_value = r.json()['records'][0]['fields']['timestamp']

print(f'Filtering data after {latest_ods_value} for submission to ODS via realtime API...')
realtime_df = merged_df[merged_df['timestamp'] > latest_ods_value]

# Realtime API bootstrap data:
# {
#   "timestamp": "2020-07-28T01:35:00+02:00",
#   "pegel": "245.16",
#   "abfluss": "591.2"
# }

print(f'Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...')
for index, row in realtime_df.iterrows():
    timestamp_text = row.timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')
    payload = {'timestamp': timestamp_text, 'pegel': row.pegel, 'abfluss': row.abfluss}
    print(f'Pushing row {index} with with the following data to ODS: {payload}')
    # r = requests.post(credentials.ods_live_push_api_url, json=payload)
    r = common.requests_post(url=credentials.ods_live_push_api_url, json=payload)
    r.raise_for_status()

print('Job successful!')
