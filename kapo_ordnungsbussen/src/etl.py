import logging
import pandas as pd
from kapo_ordnungsbussen import credentials
import common
from common import change_tracking as ct
import ods_publish.etl_id as odsp
import openpyxl
import os


def main():
    logging.info(f'Reading 2017 data from csv...')
    df_2020_07_27 = pd.read_csv(os.path.join(credentials.data_orig_path, '2020_07_27/OGD_BussenDaten.csv'), sep=';', encoding='cp1252')
    df_2020_07_27['Übertretungsdatum'] = pd.to_datetime(df_2020_07_27['Übertretungsdatum'], format='%d.%m.%Y')
    df_2017 = df_2020_07_27.query('Übertretungsjahr == 2017')

    logging.info(f'Reading 2018 data from csv...')
    df_2020_10_15 = pd.read_csv(os.path.join(credentials.data_orig_path, '2020_10_15/OGD_BussenDaten.csv'), sep=';', encoding='cp1252')
    df_2020_10_15['Übertretungsdatum'] = pd.to_datetime(df_2020_10_15['Übertretungsdatum'], format='%d.%m.%Y')
    df_2020_10_15['Übertretungsjahr'] = df_2020_10_15['Übertretungsdatum'].dt.year
    df_2018 = df_2020_10_15.query('Übertretungsjahr == 2018') # .drop(columns=credentials.columns_to_drop)

    logging.info(f'Reading 2019+ data from xslx...')
    df_ab_2019 = pd.read_excel(os.path.join(credentials.data_orig_path, '2021-03-31/OGD.xlsx'))

    df_all = pd.concat([df_2017, df_2018, df_ab_2019], ignore_index=True)
    logging.info('Calculating weekday, weekday number, and its combination...')
    df_all['Übertretungswochentag'] = df_all['Übertretungsdatum'].dt.weekday.apply(lambda x: common.weekdays_german[x])
    # Translate from Mo=0 to So=1, Mo=2 etc. to be backward.compatible with previously used SAS code
    df_all['ÜbertretungswochentagNummer'] = (df_all['Übertretungsdatum'].dt.weekday.replace({0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}))
    df_all['Wochentag'] = df_all['ÜbertretungswochentagNummer'].astype(str) + ' ' + df_all['Übertretungswochentag'].astype(str)

    logging.info('Replacing wrong PLZ...')
    df_all['Ü-Ort PLZ'] = df_all['Ü-Ort PLZ'].replace(credentials.plz_replacements).astype(int)

    logging.info('Cleaning up data for export...')
    df_all['Laufnummer'] = range(1, 1 + len(df_all))
    df_export = df_all[['Laufnummer',
                        'KAT BEZEICHNUNG',
                        'Wochentag',
                        'ÜbertretungswochentagNummer',
                        'Übertretungswochentag',
                        'Übertretungsmonat',
                        'Übertretungsjahr',
                        'GK-Limite',
                        'Ü-Ort PLZ',
                        'Ü-Ort ORT',
                        'Bussen-Betrag',
                        'BuZi',
                        'BuZi Zus.',
                        'BuZi Text',
                        ]]
    df_export = df_export.copy()
    df_export['BuZi Text'] = df_export['BuZi Text'].str.replace('"', '\'')
    # Remove newline, carriage return, and tab, see https://stackoverflow.com/a/67541987
    df_export['BuZi Text'] = df_export['BuZi Text'].str.replace(r'\r+|\n+|\t+', '', regex=True)

    df_bussen_big = df_export.query('`Bussen-Betrag` > 300')

    df_export = df_export.query('`Bussen-Betrag` > 0')
    df_export = df_export.query('`Bussen-Betrag` <= 300')

    export_filename_data = os.path.join(credentials.export_path, 'Ordnungsbussen_OGD.csv')
    logging.info(f'Exporting data to {export_filename_data}...')
    df_export.to_csv(export_filename_data, index=False)
    common.upload_ftp(export_filename_data, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'kapo/ordnungsbussen')
    odsp.publish_ods_dataset_by_id('100058')

    logging.info('Exporting data for high Bussen, and for all found PLZ...')
    df_bussen_big.to_csv(os.path.join(credentials.export_path, 'big_bussen.csv'))
    df_plz = pd.DataFrame(sorted(df_export['Ü-Ort PLZ'].unique()), columns=['Ü-Ort PLZ'])
    df_plz.to_csv(os.path.join(credentials.export_path, 'plz.csv'))


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
