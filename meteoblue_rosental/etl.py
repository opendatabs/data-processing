import logging
import common
import pandas as pd
from meteoblue_rosental import credentials
import os
from datetime import datetime
import pathlib

fields = 'timestamp,precipitation,relativeHumidityHC,solarRadiation,' \
         'airTemperatureHC,windSpeedUltraSonic,windDirUltraSonic'

url = f'http://measurement-api.meteoblue.com/v1/rawdata/pesslCityClimateBasel/cCBaselPesslMeasurement/' \
      f'get?stations=0020F940&fields={fields}&sort=desc&apikey={credentials.apikey}'


def main():
    df = get_data()
    filename = os.path.join(pathlib.Path(__file__).parent, 'data',
                            f"rosental_wetterstation_{datetime.today().strftime('%Y-%m-%d')}.csv")
    logging.info(f'Exporting data to {filename}...')
    df.to_csv(filename, index=False)
    ftp_dir = 'Rosental-Mitte/backup_wetterstation'
    logging.info(f'upload data to {ftp_dir}')
    common.upload_ftp(filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, ftp_dir)
    logging.info("push data to ODS realtime API")
    logging.info("push for dataset 100294")
    push_url = credentials.ods_live_realtime_push_url
    push_key = credentials.ods_live_realtime_push_key
    common.ods_realtime_push_df(df, url=push_url, push_key=push_key)


def get_data():
    req = common.requests_get(url)
    data = req.json()['columns']
    df_import = pd.DataFrame.from_dict(data)
    df_export = pd.DataFrame()
    for column in df_import['column']:
        df_export[column] = list(df_import.loc[(df_import['column'] == column), 'values'])[0]
    return df_export


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')

# Realtime API boostrap data:
# {"timestamp": "2023-04-13T04:00:01Z",
#     "precipitation": 0.5,
#     "relativeHumidityHC": 0.5,
#     "solarRadiation": 0.5,
#     "airTemperatureHC": 0.5,
#     "windSpeedUltraSonic": 0.5,
#     "windDirUltraSonic": 0.5}

