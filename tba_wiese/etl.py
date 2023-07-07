import logging
import os
import pathlib
import common
import pandas as pd
from tba_wiese import credentials
from zoneinfo import ZoneInfo


def main():
    r = common.requests_get(url=credentials.url, auth=(credentials.username, credentials.password))
    data = r.json()
    df = pd.DataFrame.from_dict([data])[['datum', 'temperatur']]
    df['timestamp'] = pd.to_datetime(df.datum, dayfirst=True).dt.tz_localize(ZoneInfo('Etc/GMT-1')).dt.tz_convert('UTC')
    df['timestamp_text'] = df.timestamp.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
    df_export = df[['timestamp_text', 'temperatur']]

    filename = os.path.join(os.path.dirname(__file__), 'data', f"{df_export.loc[0].timestamp_text.replace(':',' - ').replace(' ', '')}.csv")
    df_export.to_csv(filename, index=False)
    common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/wiese/temperatur')

    # {"timestamp_text": "2023-01-24T06:16:15+0000", "temperatur": "4.69"}
    r = common.ods_realtime_push_df(df_export, credentials.ods_push_url)
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
