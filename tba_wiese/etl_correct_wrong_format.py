import logging
import os
import common
import pandas as pd
from tba_wiese import credentials


def main():
    data_path = '.\data\\temperatur_wrong_format'
    # Iterate over all files in the folder
    df = pd.DataFrame()
    for file in os.listdir(data_path):
        df = pd.read_csv(os.path.join(data_path, file))
        df['timestamp_text'] = pd.to_datetime(df.timestamp_text, format="%Y-%d-%mT%H:%M:%S%z").dt.strftime('%Y-%m-%dT%H:%M:%S%z')
        filename = f"{df.loc[0].timestamp_text.replace(':', ' - ').replace(' ', '')}.csv"
        folder = filename[:7]
        filepath = os.path.join(os.path.dirname(__file__), 'data', filename)
        df.to_csv(filepath, index=False)
        common.ensure_ftp_dir(credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              f'tba/wiese/temperatur/{folder}')
        common.upload_ftp(filepath, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'tba/wiese/temperatur/{folder}')
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
