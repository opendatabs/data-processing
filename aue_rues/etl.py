import logging
import os
from more_itertools import chunked
import pandas as pd
import common
from aue_rues import credentials


def download_latest_data():
    local_path = os.path.join(os.path.dirname(__file__), 'data_orig')
    return common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'onlinedaten', local_path,  '*_RUES_Online_S3.csv', list_only=False)


def push_data_files(csv_files):
    for file in csv_files:
        df = pd.read_csv(file['local_file'], sep=';')
        # {"Startzeitpunkt": "01.01.2020 00:00:00", "Endezeitpunkt": "01.01.2020 00:15:00", "RUS.W.O.S3.LF": 384.1, "RUS.W.O.S3.O2": 12.31, "RUS.W.O.S3.PH": 8.03, "RUS.W.O.S3.TE": 6.58}
        r = common.ods_realtime_push_df(df, url=credentials.ods_push_url)


def archive_data_files(csv_files):
    archive_folder = 'archiv_ods'
    for file in csv_files:
        from_name = f"{file['remote_path']}/{file['remote_file']}"
        to_name = f"roh/{archive_folder}/{file['remote_file']}"
        logging.info(f'Renaming file on FTP server from {from_name} to {to_name}...')
        common.rename_ftp(from_name, to_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass)


def push_older_data_files():
    data_path = os.path.join(os.path.dirname(__file__), 'data_orig')

    df1 = pd.read_csv(os.path.join(data_path, 'online2002_2019.csv'), sep=';')
    df1 = add_seconds(df1)
    batched_ods_realtime_push(df1)

    df2 = pd.read_csv(os.path.join(data_path, 'Onliner_RUES_2020_2021_2022_1h_S3_OGD.csv'), sep=';', encoding='cp1252')
    df2 = df2.rename(columns={'StartZeit': 'Startzeitpunkt', 'EndeZeit': 'Endezeitpunkt', 'Temp_S3 [°C]': 'RUS.W.O.S3.TE', 'pH_S3 [-]': 'RUS.W.O.S3.PH', 'O2_S3 [mg_O2/L]': 'RUS.W.O.S3.O2', 'LF_S3 [µS/cm_25°C]': 'RUS.W.O.S3.LF'})
    df2 = df2[['Startzeitpunkt', 'Endezeitpunkt', 'RUS.W.O.S3.LF', 'RUS.W.O.S3.O2', 'RUS.W.O.S3.PH', 'RUS.W.O.S3.TE']]
    df2 = add_seconds(df2)
    df2 = df2.dropna(subset=['Startzeitpunkt', 'Endezeitpunkt'])
    batched_ods_realtime_push(df2)

    df3 = pd.read_csv(os.path.join(data_path, '2023_01_03-2023_01_05_unkorrigiert.csv'), sep=';')
    batched_ods_realtime_push(df3)
    pass


def batched_ods_realtime_push(df, chunk_size=25000):
    logging.info(f'Pushing a dataframe in chunks of size {chunk_size} to ods...')
    df_chunks = chunked(df.index, chunk_size)
    for df_chunk_indexes in df_chunks:
        logging.info(f'Submitting a data chunk to ODS...')
        df_chunk = df.iloc[df_chunk_indexes]
        r = common.ods_realtime_push_df(df_chunk, credentials.ods_push_url)


def add_seconds(df):
    df.Startzeitpunkt = df.Startzeitpunkt + ':00'
    df.Endezeitpunkt = df.Endezeitpunkt + ':00'
    return df


def main():
    # Uncomment to parse, transform and push older files (corrected etc.)
    # push_older_data_files()

    csv_files = download_latest_data()
    push_data_files(csv_files)
    archive_data_files(csv_files)

    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
