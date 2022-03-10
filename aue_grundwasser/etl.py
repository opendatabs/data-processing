import datetime
import logging
import os
import pandas as pd
from pyproj import Transformer
import common
from aue_grundwasser import credentials
import ods_publish.etl_id as odsp


def list_files():
    files = []
    for remote_path in credentials.ftp_remote_paths:
        listing = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_path, credentials.data_orig_path, '*.csv', list_only=True)
        files.extend(listing)
    return files


def process(file):
    logging.info(f'Starting reading csv into dataframe ({datetime.datetime.now()})...')
    df = pd.read_csv(file, sep=';', encoding='cp1252', low_memory=False)
    logging.info(f'Dataframe present in memory now ({datetime.datetime.now()}).')
    df['timestamp_text'] = df.Date + 'T' + df.Time
    df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%Y-%m-%dT%H:%M:%S')

    df['x'] = df.XCoord.round(0).astype(int)
    df['y'] = df.YCoord.round(0).astype(int)
    # see https://stackoverflow.com/a/65711998
    t = Transformer.from_crs('EPSG:2056', 'EPSG:4326', always_xy=True)
    x, y = t.transform(df.x.values, df.y.values)
    df['lon'] = x
    df['lat'] = y
    df['geo_point_2d'] = df.lat.astype(str).str.cat(df.lon.astype(str), sep=',')


    # df_points = gpd.GeoDataFrame(df, crs='EPSG:2056', geometry=gpd.points_from_xy(df.x, df.y))
    # # see https://epsg.io/2056
    # lv95_proj_str = '+proj=somerc +lat_0=46.95240555555556 +lon_0=7.439583333333333 +k_0=1 +x_0=2600000 +y_0=1200000 +ellps=bessel +towgs84=674.374,15.056,405.346,0,0,0,0 +units=m +no_defs '
    # df_points = gpd.GeoDataFrame(df, crs=lv95_proj_str, geometry=gpd.points_from_xy(df.x, df.y))
    #
    # d = {'col1': ['name1', 'name2'], 'wkt': ['POINT (1 2)', 'POINT (2 1)']}
    # df = pd.DataFrame(d)
    # gs = gpd.GeoSeries.from_wkt(df['wkt'])
    # gdf = gpd.GeoDataFrame(df, geometry=gs, crs=2056)

    exported_files = []
    for sensornr_filter in [10, 20]:
        logging.info(f'Processing SensorNr {sensornr_filter}...')
        df_filter = df.query('SensorNr == @sensornr_filter')
        value_filename = os.path.join(credentials.data_path, 'values', f'SensorNr_{sensornr_filter}', os.path.basename(file).replace('.csv', f'_{sensornr_filter}.csv'))
        logging.info(f'Exporting value data to {value_filename}...')
        value_columns = ['Date', 'Time', 'StationNr', 'StationName', 'SensorNr', 'SensName', 'Value', 'XCoord', 'YCoord', 'topTerrain', 'refPoint', 'Status', 'on/offline', 'timestamp_text', 'timestamp']
        df_filter[value_columns].to_csv(value_filename, index=False)
        common.upload_ftp(value_filename, credentials.ftp_server, credentials.ftp_user_up, credentials.ftp_pass_up, os.path.join(credentials.ftp_path_up, 'values', f'SensorNr_{sensornr_filter}'))
        exported_files.append(value_filename)

        stat_columns = ['StationNr', 'StationName', 'SensorNr', 'SensName', 'XCoord', 'YCoord', 'topTerrain', 'refPoint', '10YMin', '10YMean', '10YMax', 'startStatist', 'endStatist']
        df_stat = df_filter[stat_columns].drop_duplicates(ignore_index=True)
        df_stat['stat_start_timestamp'] = pd.to_datetime(df_stat.startStatist, dayfirst=True).dt.strftime(date_format='%Y-%m-%dT%H:%M:%S')
        df_stat['stat_end_timestamp'] = pd.to_datetime(df_stat.endStatist, dayfirst=True).dt.strftime(date_format='%Y-%m-%dT%H:%M:%S')
        stat_filename = os.path.join(credentials.data_path, 'stat', f'SensorNr_{sensornr_filter}', os.path.basename(file).replace('.csv', f'_{sensornr_filter}.csv'))
        logging.info(f'Exporting stat data to {stat_filename}...')
        df_stat.to_csv(stat_filename, index=False)
        common.upload_ftp(stat_filename, credentials.ftp_server, credentials.ftp_user_up, credentials.ftp_pass_up, os.path.join(credentials.ftp_path_up, 'stat', f'SensorNr_{sensornr_filter}'))
    return exported_files


def archive(file):
    to_name = os.path.join('..', credentials.ftp_archive_path, os.path.basename(file))
    common.rename_ftp(file, to_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)


def main():
    files_to_process = list_files()
    for remote_file in files_to_process:
        logging.info(f"processing {remote_file['local_file']}...")
        file = common.download_ftp([remote_file['remote_file']], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_file['remote_path'], credentials.data_orig_path, '')[0]
        process(file['local_file'])
        remote_file_with_path = os.path.join(file['remote_path'], file['remote_file'])
        archive(remote_file_with_path)
    if len(files_to_process) > 0:
        odsp.publish_ods_dataset_by_id('100164')
        odsp.publish_ods_dataset_by_id('100179')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    # testing transformation during development using a single file:
    # files = process('/Users/jonasbieri/PycharmProjects/data-processing/aue_grundwasser/data_orig/BS_Grundwasser_odExp_20220115_000000.csv')
    main()
