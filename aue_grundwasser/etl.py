import datetime
import logging
import os
import pandas as pd
import common
from aue_grundwasser import credentials


def list_files():
    files = []
    for remote_path in credentials.ftp_remote_paths:
        files = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_path, credentials.data_orig_path, '*.csv', list_only=True)
    return files


def process(file):
    logging.info(f'Starting reading csv into dataframe ({datetime.datetime.now()})...')
    df = pd.read_csv(file, sep=';', encoding='cp1252', low_memory=False)
    logging.info(f'Dataframe present in memory now ({datetime.datetime.now()}).')
    df['timestamp_text'] = df.Date + 'T' + df.Time
    df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%Y-%m-%dT%H:%M:%S')
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
    moved_files = []
    to_name = os.path.join('..', credentials.ftp_archive_path, os.path.basename(file))
    for folder in credentials.ftp_remote_paths:
        file_name = os.path.join(folder, file)
        common.rename_ftp(file_name, to_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)
        moved_files.append(to_name)
    return moved_files


def main():
    files_to_process = list_files()
    for remote_file in files_to_process:
        logging.info(f"processing {remote_file['local_file']}...")
        file = common.download_ftp([remote_file['remote_file']], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_file['remote_path'], credentials.data_orig_path, '')[0]
        process(file['local_file'])
        archive(file['remote_file'])
    # if len(remote_file) > 0:
    #   ods_publish
    #   ods_publish
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
