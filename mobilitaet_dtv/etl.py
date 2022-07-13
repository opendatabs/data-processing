import logging
import numpy as np
import pandas as pd
import common
from mobilitaet_dtv import credentials


def main():
    logging.info(f'Reading pickle data files created by kapo_geschwindigkitsmonitoring.etl:')
    logging.info(f'Reading into df from pickle {credentials.raw_metadata_filename}...')
    df_metadata_raw = pd.read_pickle(credentials.raw_metadata_filename)
    logging.info(f'Reading into df from pickle {credentials.richtung_metadata_filename}...')
    df_metadata_richtung = pd.read_pickle(credentials.richtung_metadata_filename)
    logging.info(f'Reading into df from pickle {credentials.data_filename}...')
    df_data = pd.read_pickle(credentials.data_filename)
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
    df_dtv = df_dtv_messung.merge(df_dtv_richtung, how='inner').merge(df_dtv_lk_pivot, how='inner')

    logging.info(f'calculating DTV...')
    df_dtv['dtv'] = df_dtv['count'] / df_dtv['Messdauer_h'] * 24
    df_dtv['dtv_lt_3.5m'] = df_dtv['count_lt_3.5m'] / df_dtv.Messdauer_h * 24
    df_dtv['dtv_3.5_to_lt_8m'] = df_dtv['count_3.5_to_lt_8m'] / df_dtv.Messdauer_h * 24
    df_dtv['dtv_gte_8m'] = df_dtv['count_gte_8m'] / df_dtv.Messdauer_h * 24

    # todo: Join with df_metadata_raw nand filter out measurements that should not be used for DTV

    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
