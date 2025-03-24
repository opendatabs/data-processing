import logging
import os
import numpy as np
import pandas as pd

import common
from common import change_tracking as ct
import ods_publish.etl_id as odsp
from dotenv import load_dotenv

load_dotenv()

RAW_METADATA_FILENAME = os.getenv('RAW_METADATA_FILENAME')
RICHTUNG_METADATA_FILENAME = os.getenv('RICHTUNG_METADATA_FILENAME')
DATA_FILENAME = os.getenv('DATA_FILENAME')


def main():
    if ct.has_changed(RAW_METADATA_FILENAME) or ct.has_changed(RICHTUNG_METADATA_FILENAME) or ct.has_changed(DATA_FILENAME):
        logging.info(f'Reading pickle data files created by kapo_geschwindigkitsmonitoring.etl:')
        logging.info(f'Reading into df from pickle {RAW_METADATA_FILENAME}...')
        df_metadata_raw = pd.read_pickle(RAW_METADATA_FILENAME)
        logging.info(f'Reading into df from pickle {RICHTUNG_METADATA_FILENAME}...')
        df_metadata_richtung = pd.read_pickle(RICHTUNG_METADATA_FILENAME)
        logging.info(f'Reading into df from pickle {DATA_FILENAME}...')
        df_data = pd.read_pickle(DATA_FILENAME)
        logging.info(f'Data now in memory!')

        logging.info(f'Calculating Laengenklasse...')
        bins =      [np.NINF, 3.5,    8,                 np.inf]
        # labels =    ['klein', 'mittel', 'gross']
        labels =    ['lt_3.5m',         '3.5_to_lt_8m',    'gte_8m']
        df_data['Laengenklasse'] = pd.cut(df_data['Fahrzeuglänge'], bins=bins, labels=labels, right=False, include_lowest=True)
        df_data_head = df_data.head(1000)

        logging.info(f'Calculating Messdauer by using first and last vehicle per Messung-ID...')
        df_dtv_messung = df_data.groupby(['Messung-ID'], as_index=False).agg(
            min_timestamp=('Timestamp', min),
            max_timestamp=('Timestamp', max)
        )
        df_dtv_messung['Messdauer_h'] = (df_dtv_messung.max_timestamp - df_dtv_messung.min_timestamp) / pd.Timedelta(hours=1)

        logging.info(f'Counting number of data points per Messung-ID, and Richtung ID...')
        df_dtv_richtung = df_data.groupby(['Messung-ID', 'Richtung ID'], as_index=False).agg(
            count=('Fahrzeuglänge', 'count')
        )

        logging.info(f'Calculating number of data points per Messung-ID, Richtung ID, and Laengenklasse...')
        df_dtv_laengenklasse = df_data.groupby(['Messung-ID', 'Richtung ID', 'Laengenklasse']).agg(
            count=('Fahrzeuglänge', 'count')
        ).reset_index()
        df_dtv_lk_pivot = common.collapse_multilevel_column_names(df_dtv_laengenklasse.pivot_table(index=['Messung-ID', 'Richtung ID'], columns=['Laengenklasse']).reset_index()).rename(columns={'Messung-ID_': 'Messung-ID', 'Richtung ID_': 'Richtung ID'})

        logging.info(f'Merging all dtv tables...')
        df_dtv = (df_dtv_messung.merge(df_dtv_richtung, how='inner')
                  .merge(df_dtv_lk_pivot, how='inner')
                  .merge(df_metadata_richtung, how='left')
                  )

        logging.info(f'calculating DTV...')
        df_dtv['dtv'] = df_dtv['count'] / df_dtv['Messdauer_h'] * 24
        df_dtv['dtv_lt_3.5m'] = df_dtv['count_lt_3.5m'] / df_dtv.Messdauer_h * 24
        df_dtv['dtv_3.5_to_lt_8m'] = df_dtv['count_3.5_to_lt_8m'] / df_dtv.Messdauer_h * 24
        df_dtv['dtv_gte_8m'] = df_dtv['count_gte_8m'] / df_dtv.Messdauer_h * 24

        logging.info(f'Calculating column extraordinary_traffic_routing...')
        df_metadata_raw['extraordinary_traffic_routing'] = df_metadata_raw['Messung während ausserordentlicher Verkehrsführung'].fillna(0).astype(bool)
        logging.info(f'Filtering out measurements based on Status, and joining with df_metadata_raw ...')
        df_status = df_metadata_raw.query("Status == 'Messung beendet'").rename(columns={'ID': 'Messung-ID'})
        df_dtv_status = df_status.merge(df_dtv, how='inner')

        df_export = df_dtv_status[['Messung-ID', 'Strasse', 'Strasse_Nr', 'Ort', 'the_geom',
           'extraordinary_traffic_routing', 'min_timestamp', 'max_timestamp',
           'Messdauer_h', 'Richtung ID', 'Richtung', 'count', 'count_lt_3.5m',
           'count_3.5_to_lt_8m', 'count_gte_8m', 'dtv', 'dtv_lt_3.5m',
           'dtv_3.5_to_lt_8m', 'dtv_gte_8m', 'dataset_id', 'link_zu_einzelmessungen']].copy()
        df_export['min_timestamp_text'] = df_export['min_timestamp']
        df_export['max_timestamp_text'] = df_export['max_timestamp']
        export_filename = os.path.join(os.path.dirname(__file__), 'data', 'dtv.csv')
        logging.info(f'Exporting data to {export_filename}...')
        df_export.to_csv(export_filename, index=False)
        common.update_ftp_and_odsp(export_filename, 'mobilitaet_dtv', '100199')
        ct.update_hash_file(RAW_METADATA_FILENAME)
        ct.update_hash_file(RICHTUNG_METADATA_FILENAME)
        ct.update_hash_file(DATA_FILENAME)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
