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
    for file in os.listdir(data_path):
        df = pd.read_csv(os.path.join(data_path, file))
        common.ods_realtime_push_df(df, credentials.ods_push_url)
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
