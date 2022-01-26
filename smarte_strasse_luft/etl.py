import logging
import os
from datetime import date
from common import change_tracking as ct
import pandas as pd
import common
import urllib3
import numpy as np
from smarte_strasse_luft import credentials


def main():
    logging.info(f'Handling live data...')
    live_column_name_replacements = {
        'Anfangszeit_Unnamed: 0_level_1_Unnamed: 0_level_2_Unnamed: 0_level_3_Unnamed: 0_level_4': 'Anfangszeit',
        'bl_Gundeldingerstrasse107_NO2_NO2_Sensirion_min30_µg/m3': 'G107_NO2',
        'bl_Gundeldingerstrasse107_O3_O3_Sensirion_min30_µg/m3': 'G107_03',
        'bl_Gundeldingerstrasse107_PM2.5_PM25_Sensirion_min30_ug/m3': 'G107_PM25',
        'bl_Gundeldingerstrasse125_NO2_NO2_Sensirion_min30_µg/m3': 'G125_NO2',
        'bl_Gundeldingerstrasse125_O3_O3_Sensirion_min30_µg/m3': 'G125_O3',
        'bl_Gundeldingerstrasse125_PM2.5_PM25_Sensirion_min30_ug/m3': 'G125_PM25',
        'bl_Gundeldingerstrasse131_NO2_NO2_Sensirion_min30_µg/m3': 'G131_NO2',
        'bl_Gundeldingerstrasse131_O3_O3_Sensirion_min30_µg/m3': 'G131_O3',
        'bl_Gundeldingerstrasse131_PM2.5_PM25_Sensirion_min30_ug/m3': 'G131_PM25'
    }
    etl(credentials.data_url_live, live_column_name_replacements, export_file=os.path.join(credentials.data_path, f'luft_{date.today()}.csv'), push_url=credentials.ods_live_realtime_push_url, push_key=credentials.ods_live_realtime_push_key)
    logging.info(f"Handling yesterday's data...")
    yest_column_name_replacements = {
        'Anfangszeit_Unnamed: 0_level_1_Unnamed: 0_level_2_Unnamed: 0_level_3_Unnamed: 0_level_4': 'Anfangszeit',
        'bl_Gundeldingerstrasse107_NO2_NO2_Sensiriron_d1_µg/m3': 'G107_NO2',
        'bl_Gundeldingerstrasse107_PM2.5_PM25_Sensirion_d1_ug/m3': 'G107_PM25',
        'bl_Gundeldingerstrasse107_O3_O3_Sensirion_max_h1_d1_µg/m3': 'G107_03',
        'bl_Gundeldingerstrasse125_NO2_NO2_Sensiriron_d1_µg/m3': 'G125_NO2',
        'bl_Gundeldingerstrasse125_PM2.5_PM25_Sensirion_d1_ug/m3': 'G125_PM25',
        'bl_Gundeldingerstrasse125_O3_O3_Sensirion_max_h1_d1_µg/m3': 'G125_O3',
        'bl_Gundeldingerstrasse131_NO2_NO2_Sensiriron_d1_µg/m3': 'G131_NO2',
        'bl_Gundeldingerstrasse131_PM2.5_PM25_Sensirion_d1_ug/m3': 'G131_PM25',
        'bl_Gundeldingerstrasse131_O3_O3_Sensirion_max_h1_d1_µg/m3': 'G131_O3'
    }
    etl(credentials.data_url_yest, yest_column_name_replacements, export_file=os.path.join(credentials.data_path, f'luft_yesterday_{date.today()}.csv'), push_url=credentials.ods_yest_realtime_push_url, push_key=credentials.ods_yest_realtime_push_key)


def etl(download_url, column_name_replacements, export_file, push_url, push_key):
    logging.info(f'Downloading data from {download_url}...')
    urllib3.disable_warnings()
    df = common.pandas_read_csv(download_url, sep=';', encoding='cp1252', header=[0, 1, 2, 3, 4])
    # Replace the 2-level multi-index column names with a string that concatenates both strings
    df.columns = ["_".join(str(c) for c in col) for col in df.columns.values]
    df = df.reset_index(drop=True)
    df = df.rename(columns=column_name_replacements)

    print(f'Calculating ISO8601 time string...')
    df['timestamp'] = pd.to_datetime(df.Anfangszeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')
    df = df.replace(' ', np.nan).dropna(thresh=3).reset_index(drop=True)
    row_count = len(df)
    if row_count == 0:
        print(f'No rows to push to ODS... ')
    else:
        df.to_csv(export_file, index=False)
        if ct.has_changed(export_file, do_update_hash_file=False):
            common.upload_ftp(export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'smarte_strasse/luft')
            ct.update_hash_file(export_file)

        print(f'Pushing {row_count} rows to ODS realtime API...')
        # Realtime API bootstrap data:
        # {
        #     "Anfangszeit": "09.01.2022 16:00:00",
        #     "G107_NO2": "0.5",
        #     "G107_03": "0.5",
        #     "G107_PM25": "0.5",
        #     "G125_NO2": "0.5",
        #     "G125_O3": "0.5",
        #     "G125_PM25": "0.5",
        #     "G131_NO2": "0.5",
        #     "G131_O3": "0.5",
        #     "G131_PM25": "0.5",
        #     "timestamp": "2022-01-09T16:00:00+0100",
        #     "timestamp_text": "2022-01-09T16:00:00+0100"
        # }
        df.timestamp = df.timestamp.dt.strftime('%Y-%m-%dT%H:%M:%S%z')
        df['timestamp_text'] = df.timestamp
        payload = df.to_json(orient="records")
        # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
        r = common.requests_post(url=push_url, data=payload, params={'pushkey': push_key, 'apikey': credentials.ods_api_key})
        r.raise_for_status()

    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
