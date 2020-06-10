import pandas as pd
import requests
from datetime import datetime, timedelta
import os
import common
from tba_wildedeponien import credentials
from io import StringIO

# Get all data once
# api_url = f'https://tba-bs.ch/export?object=sr_wilde_deponien_ogd&format=csv'

# Subsequently get only data since yesterday
from_timestamp = (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
api_url = f'https://tba-bs.ch/export?object=sr_wilde_deponien_ogd&from={from_timestamp}&format=csv'
print(f'Retrieving data since ({from_timestamp}) from API call to "{api_url}"...')
r = requests.get(api_url, auth=(credentials.api_user, credentials.api_password))

if r.status_code == 200:
    data = StringIO(r.text)
    df = pd.read_csv(data, sep=';')
    print('Retrieving lat and lon from column "koordinaten"...')
    df['coords'] = df.koordinaten.str.replace('POINT(', '', regex=False)
    df['coords'] = df.coords.str.replace(')', '', regex=False)
    # df['coords'] = df.coords.str.replace(' ', ',', regex=False)
    df2 = df['coords'].str.split(' ', expand=True)
    df = df.assign(lon = df2[[0]], lat = df2[[1]])

    # print('Extracting lat and long from column "koordinaten..."')
    # 'POINT\((?<long> \d *.\d *)\s(?<lat> \d *.\d *)\)'


    print('Creating ISO8601 timestamps with timezone info...')
    df['Timestamp'] = pd.to_datetime(df['bearbeitungszeit_meldung'], format='%Y-%m-%d %H:%M:%S')
    df['Timestamp'] = df['Timestamp'].dt.tz_localize('Europe/Zurich')

    timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    file_path = os.path.join(credentials.path, f'{timestamp}_{credentials.filename}')
    print(f'Exporting data to {file_path}...')
    df.to_csv(file_path, sep=';', index=False, date_format='%Y-%m-%dT%H:%M:%S%z')

    common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/wilde-deponien-tba')
    print('Job successful!')
else:
    raise Exception(f'HTTP error getting values from API: {r.status_code}')
