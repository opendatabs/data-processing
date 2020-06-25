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
from_timestamp = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
api_url = f'https://tba-bs.ch/export?object=sr_wilde_deponien_ogd&from={from_timestamp}&format=csv'
print(f'Retrieving data since ({from_timestamp}) from API call to "{api_url}"...')
r = requests.get(api_url, auth=(credentials.api_user, credentials.api_password))

if r.status_code == 200:
    if len(r.text) == 0:
        print('No data retrieved from API. Job successful!')
    else:
        data = StringIO(r.text)
        df = pd.read_csv(data, sep=';')
        print('Retrieving lat and lon from column "koordinaten"...')
        df['coords'] = df.koordinaten.str.replace('POINT(', '', regex=False)
        df['coords'] = df.coords.str.replace(')', '', regex=False)
        # df['coords'] = df.coords.str.replace(' ', ',', regex=False)
        df2 = df['coords'].str.split(' ', expand=True)
        df = df.assign(lon=df2[[0]], lat=df2[[1]])
        df.lat = pd.to_numeric(df.lat)
        df.lon = pd.to_numeric(df.lon)

        print("Rasterizing coordinates and getting rid of data we don't want to have published...")
        offset_lon = 2608700
        offset_lat = 1263200
        raster_size = 50  # 50 m raster
        df['raster_lat'] = ((df.lat - offset_lat) // raster_size) * raster_size + offset_lat
        df['raster_lon'] = ((df.lon - offset_lon) // raster_size) * raster_size + offset_lon
        # df['diff_lat'] = df.lat - df.raster_lat
        # df['diff_lon'] = df.lon - df.raster_lon
        df.drop(['koordinaten', 'coords', 'lat', 'lon', 'strasse_aue', 'hausnummer_aue'], axis=1, inplace=True)

        # print('Extracting lat and long using regex from column "koordinaten..."')
        # 'POINT\((?<long> \d *.\d *)\s(?<lat> \d *.\d *)\)'

        print('Creating ISO8601 timestamps with timezone info...')
        df['Timestamp'] = pd.to_datetime(df['bearbeitungszeit_meldung'])
        # df['Timestamp'] = pd.to_datetime(df['bearbeitungszeit_meldung'], format='%Y-%m-%d %H:%M:%S%Z')
        # df['Timestamp'] = df['Timestamp'].dt.tz_localize('Europe/Zurich')
        df['bearbeitungszeit_meldung'] = df['Timestamp']
        df.drop(['Timestamp'], axis=1, inplace=True)

        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        file_path = os.path.join(credentials.path, f'{timestamp}_{credentials.filename}')
        print(f'Exporting data to {file_path}...')
        df.to_csv(file_path, index=False, date_format='%Y-%m-%dT%H:%M:%S%z')

        common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/wilde_deponien_tba')
        print('Job successful!')
else:
    raise Exception(f'HTTP error getting values from API: {r.status_code}')
