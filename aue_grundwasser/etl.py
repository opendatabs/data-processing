import datetime
import logging
import os
import pandas as pd
from pyproj import Transformer
import common
from aue_grundwasser import credentials
import ods_publish.etl_id as odsp
from zoneinfo import ZoneInfo
import numpy as np
from io import StringIO


def list_files():
    file_list = []
    for remote_path in credentials.ftp_remote_paths:
        listing = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_path, credentials.data_orig_path, 'BS_Grundwasser_odExp_*.csv', list_only=True)
        file_list.extend(listing)
    return file_list


def process(file, x_coords_1416):
    export_stats = True
    if 'BS_Grundwasser_odExp_historische_inaktive_Messstellen.csv' in file:
        logging.info(f'Processing archive file {file}...')
        dfa = pd.read_csv(file, sep=';', names=['StationNr', 'SensorNr', 'Date_text', 'Time', 'Value'], low_memory=False)
        logging.info(f'Pre-processing archive dataframe...')
        dfa.Value = dfa.Value.replace('---', np.nan)
        dfa = dfa.dropna(subset=['Value'])
        dfa['StationId'] = dfa.StationNr.str.lstrip('0')
        dfa['StationNr'] = dfa['StationNr'].astype(str).str.zfill(10)
        dfa['Date'] = pd.to_datetime(dfa['Date_text'], dayfirst=True).dt.strftime(date_format='%Y-%m-%d')
        metadata_file = common.download_ftp(['Ergaenzende_Angaben.xlsx'], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'archiv', credentials.data_orig_path, '')[0]
        dfe = pd.read_excel(metadata_file['local_file'], dtype={'StationId': 'str'})
        df = dfa.merge(dfe, how='inner', on=['StationId'])
        df['SensName'] = np.where(df.SensorNr == 20, 'Temperatur',
                         np.where(df.SensorNr == 10, 'Grundwasserstand', ''))
        df['on/offline'] = 'offline'
        df['Status'] = 'cleansed'
        export_stats = False
    else:
        logging.info(f'Starting reading csv into dataframe ({datetime.datetime.now()})...')
        df = pd.read_csv(file, sep=';', encoding='cp1252', low_memory=False)
    logging.info(f'Dataframe present in memory now ({datetime.datetime.now()}).')
    df['timestamp_text'] = df.Date + 'T' + df.Time
    df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%Y-%m-%dT%H:%M:%S').dt.tz_localize(ZoneInfo('Etc/GMT-1')).dt.tz_convert('UTC')
    logging.info(f'Rounding LV95 coordinates as required...')
    df.XCoord = df.XCoord.round(0).astype(int)
    df.YCoord = df.YCoord.round(0).astype(int)
    logging.info(f'Replacing potentially wrong xcoord for 1416 with value "{x_coords_1416}"...')
    df.loc[(df.StationNr == '0000001416'), 'XCoord'] = x_coords_1416
    logging.info(f'transforming coords to WGS84')
    # see https://stackoverflow.com/a/65711998
    t = Transformer.from_crs('EPSG:2056', 'EPSG:4326', always_xy=True)
    df['lon'], df['lat'] = t.transform(df.XCoord.values, df.YCoord.values)
    df['geo_point_2d'] = df.lat.astype(str).str.cat(df.lon.astype(str), sep=',')
    # print(f'Created geo_point_2d column: ')
    # print(df['geo_point_2d'])
    # return

    exported_files = []
    for sensornr_filter in [10, 20]:
        logging.info(f'Processing values for SensorNr {sensornr_filter}...')
        df['StationId'] = df.StationNr.astype(str).str.lstrip('0')
        df['StationNr'] = df['StationNr'].astype(str).str.zfill(10)
        df['bohrkataster-link'] = 'https://data.bs.ch/explore/dataset/100182/table/?refine.catnum45=' + df.StationId
        df_filter = df.query('SensorNr == @sensornr_filter and StationId != "1632"')
        value_filename = os.path.join(credentials.data_path, 'values', f'SensorNr_{sensornr_filter}', os.path.basename(file).replace('.csv', f'_{sensornr_filter}.csv'))
        logging.info(f'Exporting value data to {value_filename}...')
        value_columns = ['Date', 'Time', 'StationNr', 'StationId', 'StationName', 'SensorNr', 'SensName', 'Value', 'lat', 'lon', 'geo_point_2d', 'XCoord', 'YCoord', 'topTerrain', 'refPoint', 'Status', 'on/offline', 'timestamp_text', 'timestamp', 'bohrkataster-link']
        df_filter[value_columns].to_csv(value_filename, index=False)
        common.upload_ftp(value_filename, credentials.ftp_server, credentials.ftp_user_up, credentials.ftp_pass_up, '/'.join([credentials.ftp_path_up, 'values', f'SensorNr_{sensornr_filter}']))
        exported_files.append(value_filename)

        if export_stats:
            logging.info(f'Processing stats for SensorNr {sensornr_filter}...')
            stat_columns = ['StationNr', 'StationId', 'StationName', 'SensorNr', 'SensName', 'lat', 'lon', 'geo_point_2d', 'XCoord', 'YCoord', 'topTerrain', 'refPoint', '10YMin', '10YMean', '10YMax', 'startStatist', 'endStatist', 'bohrkataster-link']
            df_stat = df_filter[stat_columns].drop_duplicates(ignore_index=True)
            df_stat['stat_start_timestamp'] = pd.to_datetime(df_stat.startStatist, dayfirst=True).dt.tz_localize(ZoneInfo('Etc/GMT-1')).dt.tz_convert('UTC')
            df_stat['stat_end_timestamp'] = pd.to_datetime(df_stat.endStatist, dayfirst=True).dt.tz_localize(ZoneInfo('Etc/GMT-1')).dt.tz_convert('UTC')
            stat_filename = os.path.join(credentials.data_path, 'stat', f'SensorNr_{sensornr_filter}', os.path.basename(file).replace('.csv', f'_{sensornr_filter}.csv'))
            logging.info(f'Exporting stat data to {stat_filename}...')
            df_stat.to_csv(stat_filename, index=False)
            common.upload_ftp(stat_filename, credentials.ftp_server, credentials.ftp_user_up, credentials.ftp_pass_up, '/'.join([credentials.ftp_path_up, 'stat', f'SensorNr_{sensornr_filter}']))
        else:
            logging.info(f'Skipped processing stats for the current file and sensor {sensornr_filter}...')
    return exported_files


def archive(file):
    to_name = os.path.basename(file).replace('BS_Grundwasser_odExp_', 'BS_Grundwasser_odProc_')
    logging.info(f'Renaming file on FTP server from {file} to {to_name}...')
    common.rename_ftp(file, to_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)


def retrieve_1416_x_coordinates():
    # bohrkataster_url = 'https://data.bs.ch/explore/dataset/100182/download/?format=csv&use_labels_for_header=true&refine.catnum45=1416'
    # logging.info(f'Retrieving Bohrkataster data for laufnr 1416 from {bohrkataster_url}...')
    # r = common.requests_get(bohrkataster_url)
    # df = pd.read_csv(StringIO(r.text), sep=';')
    # df_export = df[['Laufnummer', 'Geo Point', 'X-Koordinate', 'Y-Koordinate']].rename(columns={'Geo Point': 'GeoPoint2d', 'X-Koordinate': 'XCoord', 'Y-Koordinate': 'YCoord'})
    # return df_export.iloc[0]['XCoord']

    # Coordinate will not change, return coordinate retrieved from shape through map.geo.bs.ch interface here
    return 2611900


def main():
    x_coord_1416 = 0
    files_to_process = list_files()
    # do not process file BS_Grundwasser_odExp_20210818_000000.csv (modified or added on 2023-06-20) with different date format for now
    files_to_process = [item for item in files_to_process if '20210818' not in item['remote_file'] or '2023-06-20' not in item['modified_remote']]
    if len(files_to_process) > 0:
        x_coord_1416 = retrieve_1416_x_coordinates()
    files = []
    for remote_file in files_to_process:
        logging.info(f"processing {remote_file['local_file']}...")
        file = common.download_ftp([remote_file['remote_file']], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_file['remote_path'], credentials.data_orig_path, '')[0]
        process(file['local_file'], x_coord_1416)
        files.append(file)
    if len(files_to_process) > 0:
        for ods_id in ['100164', '100179', '100180', '100181']:
            odsp.publish_ods_dataset_by_id(ods_id)
            pass
    for file in files:
        archive(os.path.join(file['remote_path'], file['remote_file']))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
