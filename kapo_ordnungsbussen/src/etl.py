import logging
import pandas as pd
from kapo_ordnungsbussen import credentials
import common
from common import change_tracking as ct
import ods_publish.etl_id as odsp
import openpyxl
import os
import datetime as datetime


def main():
    directories = list_directories()
    list_path = os.path.join(credentials.data_orig_path, 'list_directories.txt')
    if ct.has_changed(list_path):
        ct.update_hash_file(list_path)
        df_2017 = process_data_2017()
        df_all = process_data_from_2018(directories, df_2017)
        df_export = transform_for_export(df_all)
        big_bussen = os.path.join(credentials.export_path, 'big_bussen.csv')
        plz = os.path.join(credentials.export_path, 'plz.csv')
        if ct.has_changed(big_bussen):
            ct.update_hash_file(big_bussen)
            # add email
        elif ct.has_changed(plz):
            ct.update_hash_file(plz)
            # add email
        else:
            export_filename_data = os.path.join(credentials.export_path, 'Ordnungsbussen_OGD.csv')
            logging.info(f'Exporting data to {export_filename_data}...')
            df_export.to_csv(export_filename_data, index=False)


def list_directories():
    folder_path = credentials.data_orig_path
    directories = [f for f in os.listdir(folder_path) if os.path.isdir(os.path.join(folder_path, f))]
    list_path = os.path.join(credentials.data_orig_path, 'list_directories.txt')
    with open(list_path, 'w') as file:
        for item in directories:
            file.write(item + '\n')
    list_directories = [x for x in directories if x not in ['Old', 'export', '2020_07_27']]
    list_directories.sort()
    return list_directories


def process_data_2017():
    logging.info(f'Reading 2017 data from csv...')
    df_2020_07_27 = pd.read_csv(os.path.join(credentials.data_orig_path, '2020_07_27/OGD_BussenDaten.csv'), sep=';', encoding='cp1252')
    df_2020_07_27['Übertretungsdatum'] = pd.to_datetime(df_2020_07_27['Übertretungsdatum'], format='%d.%m.%Y')
    df_2017 = df_2020_07_27.query('Übertretungsjahr == 2017')
    return df_2017


def process_data_from_2018(list_directories, df_2017):
    logging.info(f'Reading 2018+ data from xslx...')
    df_all = df_2017
    for directory in list_directories:
        file = os.path.join(credentials.data_orig_path, directory, 'OGD.xlsx')
        logging.info(f'process data from file {file}')
        df = pd.read_excel(file)
        # want to take the data from the latest file, so remove in the df I have up till now all data of datum_min and after
        datum_min = df['Übertretungsdatum'].min()
        logging.info(f'Earliest date is {datum_min}, add new data from this date on (and remove data after this date coming from older files)')
        df_all = df_all[df_all['Übertretungsdatum'] < datum_min]
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def transform_for_export(df_all):
    logging.info('Calculating weekday, weekday number, and its combination...')
    df_all['Übertretungswochentag'] = df_all['Übertretungsdatum'].dt.weekday.apply(lambda x: common.weekdays_german[x])
    # Translate from Mo=0 to So=1, Mo=2 etc. to be backward.compatible with previously used SAS code
    df_all['ÜbertretungswochentagNummer'] = (df_all['Übertretungsdatum'].dt.weekday.replace({0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}))
    df_all['Wochentag'] = df_all['ÜbertretungswochentagNummer'].astype(str) + ' ' + df_all['Übertretungswochentag'].astype(str)

    logging.info('Replacing wrong PLZ...')
    df_all['Ü-Ort PLZ'] = df_all['Ü-Ort PLZ'].replace(credentials.plz_replacements).astype(int)

    logging.info(f'Replacing old BuZi with new ones using lookup table...')
    df_lookup = pd.read_excel(os.path.join(credentials.data_orig_path, '2022_06_30', 'Lookup-Tabelle BuZi.xlsx'))
    df_all['BuZi'] = df_all['BuZi'].replace(df_lookup.ALT.to_list(), df_lookup.NEU.to_list())

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
    logging.info('Exporting data for high Bussen, and for all found PLZ...')
    df_bussen_big.to_csv(os.path.join(credentials.export_path, 'big_bussen.csv'))
    df_plz = pd.DataFrame(sorted(df_export['Ü-Ort PLZ'].unique()), columns=['Ü-Ort PLZ'])
    df_plz.to_csv(os.path.join(credentials.export_path, 'plz.csv'))
    return df_export


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
