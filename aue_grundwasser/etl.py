import datetime
import ftplib
import logging
import os
import pandas as pd
import common
from aue_grundwasser import credentials


def download():
    files = []
    for remote_path in credentials.ftp_remote_paths:
        files = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, remote_path, credentials.data_orig_path, '*.csv', list_only=True)
    return files


def process(file):
    logging.info(f'Starting reading csv into dataframe ({datetime.datetime.now()})...')
    df = pd.read_csv(file, sep=';', encoding='cp1252', low_memory=False)
    logging.info(f'Dataframe present in memory now ({datetime.datetime.now()}).')
    df['timestamp_text'] = df.Date + 'T' + df.Time
    df['timestamp'] = df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%Y-%m-%dT%H:%M:%S')
    exported_files = []
    for sensornr_filter in [10, 20]:
        df_filter = df.query('SensorNr == @sensornr_filter')
        export_filename = os.path.join(credentials.data_path, f'SensorNr_{sensornr_filter}', os.path.basename(file).replace('.csv', f'_{sensornr_filter}.csv'))
        logging.info(f'Exporting data to {export_filename}...')
        df_filter.to_csv(export_filename, index=False)
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user_up, credentials.ftp_pass_up, os.path.join(credentials.ftp_path_up, f'SensorNr_{sensornr_filter}'))
        exported_files.append(export_filename)
    return exported_files


def archive(file):
    moved_files = []
    to_name = os.path.join('..', credentials.ftp_archive_path, os.path.basename(file))
    for folder in credentials.ftp_remote_paths:
        file_name = os.path.join(folder, file)
        if rename_ftp(file_name, to_name):
            moved_files.append(to_name)
        else:
            logging.error(f'File to rename on FTP not found: {file}...')
            raise FileNotFoundError(file)

    #     for file_name, facts in ftp.mlsd():
    #         if file_name == file_name:
    #             found = True
    #             to_name = os.path.join(credentials.ftp_archive_path, file_name)
    #             logging.info(f'Moving file to {to_name}...')
    #             ftp.rename(file_name, f'../{to_name}')
    #             moved_files.append(to_name)
    #     ftp.cwd('..')
    #     ftp.quit()
    # if not found:
    #     raise FileNotFoundError(file)
    return moved_files


def rename_ftp(from_name, to_name):
    file = os.path.basename(from_name)
    folder = os.path.dirname(from_name)
    ftp = ftplib.FTP(credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)
    print(f'Changing to remote dir {folder}...')
    ftp.cwd(folder)
    print('Searching for file to rename or move...')
    moved = False
    for remote_file, facts in ftp.mlsd():
        if file == remote_file:
            logging.info(f'Moving file to {to_name}...')
            ftp.rename(file, to_name)
            moved = True
            break
    ftp.quit()
    return moved


def main():
    files = download()
    for file_dict in files:
        logging.info(f"processing {file_dict['local_file']}...")
        process(file_dict['local_file'])
        archive(file_dict['remote_file'])
    # if len(file_dict) > 0:
    #   ods_publish
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    process(credentials.test_file_name)
