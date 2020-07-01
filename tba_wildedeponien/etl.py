import pandas as pd
import geopandas as gpd
import requests
from datetime import datetime, timedelta
import os
import common
from tba_wildedeponien import credentials
from io import StringIO


# Subsequently get only data since yesterday
from_timestamp = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
api_url = f'https://tba-bs.ch/export?object=sr_wilde_deponien_ogd&from={from_timestamp}&format=csv'

# Or: get all data once
# from_timestamp = 'ever'
# api_url = f'https://tba-bs.ch/export?object=sr_wilde_deponien_ogd&format=csv'

print(f'Retrieving data since {from_timestamp} from API call to "{api_url}"...')
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

        print('Reading Bezirk data into geopandas df...')
        # see e.g. https://stackoverflow.com/a/58518583/5005585
        df_wv = gpd.read_file('https://data.bs.ch/explore/dataset/100042/download/?format=geojson')
        df_bez = gpd.read_file('https://data.bs.ch/explore/dataset/100039/download/?format=geojson')
        df_points = gpd.GeoDataFrame(df, crs="EPSG:2056", geometry=gpd.points_from_xy(df.raster_lon, df.raster_lat))
        print('Reprojecting points...')
        df_points = df_points.to_crs('EPSG:4326')
        print('Spatially joining points with Wohnviertel...')
        gdf_wv = gpd.sjoin(df_points, df_wv, how='left', op="within", rsuffix='wv', lsuffix='points')
        print('Spatially joining points with Bezirk...')
        gdf_wv_bez = gpd.sjoin(gdf_wv, df_bez, how='left', op="within", rsuffix='bez', lsuffix='points')
        print('Dropping unnecessary columns...')
        gdf_wv_bez.drop(columns=['index_wv', 'index_bez', 'wov_id_points'], inplace=True)

        # todo: Find nearest Wohnviertel / Bezirk of points outside ofthos shapes (Rhein, Outside of BS territory)
        # e.g. see https://gis.stackexchange.com/a/342489

        timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        file_path = os.path.join(credentials.path, f'{timestamp}_{credentials.filename}')
        print(f'Exporting data to {file_path}...')
        gdf_wv_bez.to_csv(file_path, index=False, date_format='%Y-%m-%dT%H:%M:%S%z')

        common.upload_ftp(file_path, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/illegale-deponien')
        print('Job successful!')
else:
    raise Exception(f'HTTP error getting values from API: {r.status_code}')
