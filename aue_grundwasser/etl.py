import datetime
import ftplib
import logging
import os
import pandas as pd
from aue_grundwasser import credentials


def download():
    ftp = ftplib.FTP(credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)
    local_files = []
    for folder in credentials.ftp_remote_paths:
        print(f'Changing to remote dir {folder}...')
        ftp.cwd(folder)
        print('Retrieving list of files...')
        for file_name, facts in ftp.mlsd():
            if file_name.endswith('.csv'):
                print(f'Downloading {file_name}...')
                local_file = os.path.join(credentials.path, file_name)
                with open(local_file, 'wb') as f:
                    ftp.retrbinary(f"RETR {file_name}", f.write)
                local_files.append(local_file)
        ftp.cwd('..')
    ftp.quit()
    return local_files


def process_file(file):
    logging.info(f'Starting reading csv into dataframe ({datetime.datetime.now()})...')
    df = pd.read_csv(file, sep=';', encoding='cp1252', low_memory=False)
    logging.info(f'Dataframe present in memory now ({datetime.datetime.now()}).')
    df['timestamp_text'] = df.Date + 'T' + df.Time
    df['timestamp'] = df['timestamp'] = pd.to_datetime(df.timestamp_text, format='%Y-%m-%dT%H:%M:%S')
    pass


def process_and_push(local_files):
    for file in local_files:
        logging.info(f'processing {file}...')
        process_file(file)
    pass


def archive_files(local_files):
    pass


def main():
    local_files = download()
    process_and_push(local_files)
    archive_files(local_files)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    # main()
    process_file(credentials.test_huge_file_name)

