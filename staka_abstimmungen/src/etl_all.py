import pandas as pd
import os
import glob
import pathlib
import common
from common import change_tracking as ct
from staka_abstimmungen import credentials
import ods_publish.etl_id as odsp
import logging


"""
script to create a new dataset from all published datasets 
"""

path_files = os.path.join(pathlib.Path(__file__).parents[1], 'data/data-processing-output')

def main():
    df_all = process_files()
    df_all = construct_dataset(df_all)
    df_all = harmonize_df(df_all)
    path_export = os.path.join(pathlib.Path(__file__).parents[1], 'data/export', 'abstimmungen.csv')
    df_all.to_csv(path_export, index=False)
    if ct.has_changed(path_export):
        common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user,
                          credentials.ftp_pass, 'wahlen_abstimmungen/abstimmungen_merged')
        ct.update_hash_file(path_export)
        odsp.publish_ods_dataset_by_id('100303')


def process_files():
    logging.info('process all files')
    df = pd.DataFrame()
    files_details = get_files_details()
    files_kennzahlen = get_files_kennzahlen()
    for file in files_details:
        df_file = pd.read_csv(file)
        df = pd.concat([df, df_file])
    df = join_wahllokale(df)
    for file in files_kennzahlen:
        df_file = pd.read_csv(file)
        df = pd.concat([df, df_file])
    return df


def construct_dataset(df):
    logging.info('add column "auf_ebene"')
    df['auf_ebene'] = ['Gemeinde' if x else 'Wahllokal' for x in pd.isna(df['Wahllok_name'])]
    # add BS-ID
    logging.info('add BS-ID')
    df['year'] = (df['Abst_Datum'].astype('datetime64[ns]')).dt.year
    dict_laufnr = make_dict_date_laufnr()
    df['lauf_nr_termin'] = [dict_laufnr[datum] for datum in df['Abst_Datum']]
    df['bs_id'] = df['year'].astype(str) + '0' + df['lauf_nr_termin'] + '0' + df['Abst_ID'].astype(str)
    # df['bs_id'] = [bs_id[i] if x == "kantonal" else '' for i, x in df['Abst_Art'].items()]
    path_export = os.path.join(pathlib.Path(__file__).parents[1], 'data/export', 'abstimmungen.csv')
    df.to_csv(path_export, index=False)
    return df


def get_files_kennzahlen():
    logging.info('list all files of the Kennzahlen datasets')
    pattern_kennzahlen = 'Abstimmungen_??????????.csv'
    file_list = glob.glob(os.path.join(path_files, pattern_kennzahlen))
    return file_list


def get_files_details():
    logging.info('list all files of the Details datasets')
    pattern_details = 'Abstimmungen_Details_??????????.csv'
    file_list = glob.glob(os.path.join(path_files, pattern_details))
    return file_list


def join_wahllokale(df):
    # To do: remove or extend with wahllokale for electronic voting..
    logging.info('add wahllokale')
    path_wahllokale = os.path.join(path_files, 'wahllokale.csv')
    df_wahllokale = pd.read_csv(path_wahllokale, sep=';')
    df = pd.merge(df_wahllokale, df, left_on='Wahllok_Name', right_on='Wahllok_name')
    df = df.drop(columns='Wahllok_Name')
    return df


def harmonize_df(df):
    logging.info('harmonize some colums')
    df['Result_Art'] = ['Schlussresultat' if x == 'Schlussresultate' else x for x in df['Result_Art']]
    df['Gemein_Name'] = ['Auslandschweizer/-innen' if x == 'Auslandschweizer' else x for x in df['Gemein_Name']]
    # fill column 'Abst_ID_Titel' (for some it is empty)
    df['Abst_ID_Titel'] = df['Abst_ID'].astype(str) + ': ' + df['Abst_Titel']
    return df


def get_dates():
    logging.info('list dates of all available Abstimmungen')
    file_list = get_files_kennzahlen()
    list_dates = []
    for file in file_list:
        date_str = os.path.basename(file).split("_", 1)[1][:10]
        list_dates.append(date_str)
    list_dates.sort()
    return list_dates


def make_dict_date_laufnr():
    logging.info('determine laufnr for all dates')
    dates = get_dates()
    dict_date_laufnr = {}
    year = '2020'
    lauf_nr_termin = 2
    for date in dates:
        if date[0:4] == year:
            dict_date_laufnr[date] = str(lauf_nr_termin)
        else:
            year = date[0:4]
            lauf_nr_termin = 1
            dict_date_laufnr[date] = str(lauf_nr_termin)
        lauf_nr_termin += 1
    return dict_date_laufnr


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job successfully completed!')



# ODS realtime push bootstrap:
# {"Wahllok_ID":6.0,
#  "Gemein_ID":2.0,
#  "Gemein_Name":"Riehen",
#  "Wahllok_name":"Riehen brieflich Stimmende",
#  "Stimmr_Anz":9197.0,
#  "Eingel_Anz":9188.0,
#  "Leer_Anz":373.0,
#  "Unguelt_Anz":5.0,
#  "Guelt_Anz":8810.0,
#  "Ja_Anz":2836.0,
#  "Nein_Anz":5974.0,
#  "Abst_Titel":"Justiz-Initiative",
#  "Abst_Art":"national",
#  "Abst_Datum":"2021-11-28",
#  "Result_Art":"Schlussresultat",
#  "Abst_ID":2,
#  "anteil_ja_stimmen":0.321906924,
#  "abst_typ":"Text",
#  "Abst_ID_Titel":"2: «Justiz-Initiative»",
#  "Gege_Ja_Anz":1.0,
#  "Gege_Nein_Anz":1.0,
#  "Sti_Initiative_Anz":1.0,
#  "Sti_Gegenvorschlag_Anz":1.0,
#  "gege_anteil_ja_Stimmen":1.0,
#  "sti_anteil_init_stimmen":1.0,
#  "Init_OGA_Anz":1.0,
#  "Gege_OGA_Anz":1.0,
#  "Sti_OGA_Anz":1.0,
#  "Durchschn_Stimmbet_pro_Abst_Art":1.0,
#  "Durchschn_Briefl_Ant_pro_Abst_Art":1.0,
#  "Stimmber_Anz_M":1.0,
#  "Stimmber_Anz_F":1.0,
#  "Stimmber_Anz":1.0,
#  "auf_ebene":"Wahllokal",
#  "year":2021,
#  "lauf_nr_termin":"4",
#  "bs_id":"20210402"
#   }