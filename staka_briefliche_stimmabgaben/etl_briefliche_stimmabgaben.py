import pandas as pd
from staka_briefliche_stimmabgaben import credentials
import common
from common import change_tracking as ct
import logging
import os
import glob
from datetime import datetime


def main():
    logging.info('get previous data, starting from 07-03-2021')
    df_2020 = get_data_2020()
    df_publ = get_previous_data_from_20210307()
    df_publ = pd.concat([df_publ, df_2020], ignore_index=True)
    logging.info('get file and date of latest available file')
    latest_file, datetime_abst = get_latest_file_and_date()
    date_abst = str(datetime_abst.date())
    logging.info(f'date of latest Abstimmung is {date_abst}')
    # to do: check if this is the date of currently active Abstimmung...
    dates = [str(x.date()) for x in df_publ['abstimmungsdatum']]
    logging.info('check if data from latest Abstimmung is already in the df, if not add it')
    if date_abst not in dates:
        logging.info(f'Add data of currently active Abstimmung of {date_abst}')
        df_latest = make_df_for_publ(latest_file=latest_file, datetime_abst=datetime_abst)
        df_publ = pd.concat([df_latest, df_publ], ignore_index=True)
    # add 'tage_bis_abst'
    df_publ['tage_bis_abst'] = df_publ['abstimmungsdatum'] - df_publ['datum']
    df_publ['tage_bis_abst'] = [x.days for x in df_publ['tage_bis_abst']]
    # make date columns of string type
    df_publ['datum'] = df_publ['datum'].dt.strftime('%Y-%m-%d')
    df_publ['abstimmungsdatum'] = [str(x) for x in df_publ['abstimmungsdatum']]

    # upload csv files
    logging.info(f'upload csv file to {credentials.path_export_file_publ}')
    df_publ.to_csv(credentials.path_export_file_publ, index=False)

    # push df_publ
    if ct.has_changed(credentials.path_export_file_publ):
        ct.update_hash_file(credentials.path_export_file_publ)
        logging.info("push data to ODS realtime API")
        logging.info("push for dataset 100223")
        push_url = credentials.ods_live_realtime_push_url_publ
        push_key = credentials.ods_live_realtime_push_key_publ
        common.ods_realtime_push_df(df_publ, url=push_url, push_key=push_key)


def get_data_2020():
    pattern = '2020_Eingang_Stimmabgaben_Basel*_2020.xlsx'
    file_list = glob.glob(os.path.join(f'{credentials.path_stimmabgaben}/2020', pattern))
    df_all = pd.DataFrame()
    for file in file_list:
        tabs = pd.ExcelFile(file).sheet_names
        datetime_abst = tabs[0].split("_", 1)[0]
        datetime_abst = datetime.strptime(datetime_abst, '%d.%m.%Y')
        df = make_df_for_publ(latest_file=file, datetime_abst=datetime_abst)
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def get_previous_data_from_20210307():
    pattern = '????????_Eingang_Stimmabgaben*morgen.xlsx'
    file_list = glob.glob(os.path.join(credentials.path_stimmabgaben, pattern))
    df_all = pd.DataFrame()
    for file in file_list:
        datetime_abst = os.path.basename(file).split("_", 1)[0]
        datetime_abst = datetime.strptime(datetime_abst, '%Y%m%d')
        df = make_df_for_publ(latest_file=file, datetime_abst=datetime_abst)
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def get_latest_file_and_date():
    pattern = '????????_Eingang_Stimmabgaben*.xlsx'
    data_file_names = []
    file_list = glob.glob(os.path.join(credentials.path_stimmabgaben, pattern))
    if len(file_list) > 0:
        latest_file = max(file_list, key=os.path.getmtime)
        data_file_names.append(os.path.basename(latest_file))
    datetime_abst = data_file_names[0].split("_", 1)[0]
    datetime_abst = datetime.strptime(datetime_abst, '%Y%m%d')
    return latest_file, datetime_abst


def make_df_for_publ(latest_file, datetime_abst):
    columns = ['tag', 'datum', 'eingang_pro_tag', 'eingang_kumuliert', 'stimmbeteiligung']
    df_stimmabgaben = pd.read_excel(latest_file,
                                    sheet_name=0,
                                    header=None,
                                    names=columns,
                                    skiprows=6
                                    )
    df_stimmabgaben['stimmbeteiligung'] = 100 * df_stimmabgaben['stimmbeteiligung']
    # add column Abstimmungsdatum
    df_stimmabgaben["abstimmungsdatum"] = datetime_abst
    # remove empty rows
    df_stimmabgaben = df_stimmabgaben.dropna()
    return df_stimmabgaben


# Realtime API bootstrap data df_publ:
# {
# "tag": "Mittwoch",
#      "datum": "2022-05-15",
#      "eingang_pro_tag" : 1,
#      "eingang_kumuliert" : 1,
#     "stimmbeteiligung": 1.0,
#      "abstimmungsdatum": "2022-05-15"
#       "tage_bis_abst": 1
# }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
