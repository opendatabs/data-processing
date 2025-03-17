import logging
import common
import pandas as pd
import os
from datetime import datetime
import pathlib

from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
PUSH_URL = os.getenv("PUSH_URL")
PUSH_KEY = os.getenv("PUSH_KEY")

def main():
    df = get_data()
    filename = os.path.join(pathlib.Path(__file__).parent, 'data',
                            f"rosental_wetterstation_{datetime.today().strftime('%Y-%m-%d')}.csv")
    logging.info(f'Exporting data to {filename}...')
    df.to_csv(filename, index=False)
    ftp_dir = 'Rosental-Mitte/backup_wetterstation'
    logging.info(f'upload data to {ftp_dir}')
    common.upload_ftp(filename, FTP_SERVER, FTP_USER, FTP_PASS, ftp_dir)
    logging.info("push data to ODS realtime API to dataset 100294")
    common.ods_realtime_push_df(df, url=PUSH_URL, push_key=PUSH_KEY)


def get_data():
    provider = 'pesslCityClimateBasel'
    url = f'https://measurements-api.meteoblue.com/v2/provider/{provider}/measurement/get'
    params = {
        'stations': ['0020F940'],
        'fields': [
            'timestamp',
            'precipitation',
            'relativeHumidity_unknown',
            'globalRadiation',
            'airTemperature_unknown',
            'windSpeed_unknown',
            'windDirection'
            ],
        'sort': 'desc',
        'velocityUnit': 'm/s',
        'limit': '1000',
        'apikey': API_KEY
    }
    req = common.requests_get(url, params)
    data = req.json()['columns']
    df_import = pd.DataFrame.from_dict(data)
    df_export = pd.DataFrame()
    for column in df_import['column']:
        df_export[column] = list(df_import.loc[(df_import['column'] == column), 'values'])[0]
    # Rename columns since the names are still from the API v1
    df_export.rename(columns={
        'relativeHumidity_unknown': 'relativeHumidityHC',
        'globalRadiation': 'solarRadiation',
        'airTemperature_unknown': 'airTemperatureHC',
        'windSpeed_unknown': 'windSpeedUltraSonic',
        'windDirection': 'windDirUltraSonic'
    }, inplace=True)
    df_export['timestamp'] = pd.to_datetime(df_export['timestamp'], unit='s').dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    return df_export


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
