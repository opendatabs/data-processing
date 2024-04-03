import logging
import os
import pathlib
import common
import pandas as pd
from tba_wiese import credentials
from zoneinfo import ZoneInfo


def main():
    # Comment out to upload backup
    # upload_backup()
    r = common.requests_get(url=credentials.url, auth=(credentials.username, credentials.password))
    data = r.json()
    df = pd.DataFrame.from_dict([data])[['datum', 'temperatur']]
    df['timestamp'] = pd.to_datetime(df.datum, format="%Y-%m-%d %H:%M:%S").dt.tz_localize(ZoneInfo('Etc/GMT-1')).dt.tz_convert('UTC')
    df['timestamp_text'] = df.timestamp.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    df_export = df[['timestamp_text', 'temperatur']]
    common.ods_realtime_push_df(df_export, credentials.ods_push_url)
    filename = f"{df_export.loc[0].timestamp_text.replace(':', ' - ').replace(' ', '')}.csv"
    folder = filename[:7]
    filepath = os.path.join(os.path.dirname(__file__), 'data', filename)
    df_export.to_csv(filepath, index=False)
    common.ensure_ftp_dir(credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'tba/wiese/temperatur/{folder}')
    common.update_ftp_and_odsp(filepath, f'tba/wiese/temperatur/{folder}', '100269')
    pass


def upload_backup():
    data_path = os.path.join(pathlib.Path(__file__).parent.absolute(), 'data')
    # Iterate over month starting from january 2023 to now with while loop
    date = pd.Timestamp('2023-01-01')
    while date < pd.Timestamp.now():
        folder = date.strftime('%Y-%m')
        list_files = common.download_ftp([], credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                         f'tba/wiese/temperatur/{folder}', data_path, '*.csv')
        for file in list_files:
            file_path = file['local_file']
            df = pd.read_csv(file_path)
            common.ods_realtime_push_df(df, credentials.ods_push_url)
        date = date + pd.DateOffset(months=1)
    quit()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
