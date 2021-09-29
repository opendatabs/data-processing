import logging
import warnings
from datetime import datetime
from luftqualitaet_ch import credentials
import pandas as pd
import urllib3
from more_itertools import chunked
import common
from common import change_tracking as ct
import ods_publish.etl_id as odsp
import os


def main():
    today_string = datetime.today().strftime('%d.%m.%Y')
    payload = {
        'jsform': 'true',
        'querytype': 'station',
        'station': 'bsBSJ',
        'pollutants[]': ['PM10', 'PM2.5', 'O3', 'NO2'],
        'station_interval': 'hour',
        'station_output': 'csv',
        'pollutant_interval': 'hour',
        'pollutant_output': 'interactive',
        'timerange': 'custom',
        'startdate': '01.01.2000',
        'stopdate': today_string,
        'submit': 'Abfrage',
    }
    logging.info('Requesting data from web service...')
    urllib3.disable_warnings()
    r = common.requests_post(url='https://luftqualitaet.ch/messdaten/datenarchiv/abfrage', data=payload, verify=False)
    warnings.resetwarnings()

    raw_file = os.path.join(credentials.data_path, 'Luftqualitaet_ch-basel-st_johannplatz-raw.csv')
    logging.info(f'Writing data into file {raw_file}...')
    with open(raw_file, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)

    if ct.has_changed(raw_file):
        logging.info('Reading csv into df...')
        # Some lines have more than 5 columns, ignoring those.
        df = pd.read_csv(raw_file, skiprows=5, encoding='cp1252', sep=';', error_bad_lines=False)
        df = df.rename(columns={})
        logging.info('Removing empty lines...')
        cols = df.columns.to_list()
        data_cols = list(filter(lambda item: item not in ['Datum/Zeit'], cols))
        df = df.dropna(how='all', subset=data_cols)
        logging.info('Renaming columns...')
        df = df.rename(columns={'Datum/Zeit': 'datum_zeit',
                                'PM10 (Stundenmittelwerte  [µg/m³])': 'pm10_stundenmittelwerte_ug_m3',
                                'O3 (Stundenmittelwerte  [µg/m³])': 'o3_stundenmittelwerte_ug_m3',
                                'NO2 (Stundenmittelwerte  [µg/m³])': 'no2_stundenmittelwerte_ug_m3',
                                'PM2.5 (Stundenmittelwerte  [µg/m³])': 'pm2_5_stundenmittelwerte_ug_m3'
                                })
        export_file = os.path.join(credentials.data_path, 'Luftqualitaet_ch-basel-st_johannplatz.csv')
        df.to_csv(export_file, index=False)
        if ct.has_changed(export_file):
            common.upload_ftp(export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'lufthygiene_ch')
            odsp.publish_ods_dataset_by_id('100049')

        # todo: ODS use realtime API to push new data.
        # If data is too big to be pushed to realtime api, create chunks < 5 mb: https://newbedev.com/pandas-slice-large-dataframe-in-chunks

        # Realtime API bootstrap data:
        # {
        #     "datum_zeit": "2000-01-01 01:00:00",
        #     "pm10_stundenmittelwerte_ug_m3": "29.851",
        #     "o3_stundenmittelwerte_ug_m3": "5.426",
        #     "no2_stundenmittelwerte_ug_m3": "42.994",
        #     "pm2_5_stundenmittelwerte_ug_m3": "15.618"
        # }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
