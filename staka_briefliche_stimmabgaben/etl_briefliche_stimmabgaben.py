import pandas as pd
from staka_briefliche_stimmabgaben import credentials
import common
from common import change_tracking as ct
import logging
import os
import glob
from datetime import datetime
import locale
import numpy as np

# datetime in German
# MAC:
locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
# Windows:
# locale.setlocale(
#     category=locale.LC_ALL,
#     locale="German"  # Note: do not use "de_DE" as it doesn't work
# )

def main():
    df_publ = get_previous_data_from_20210307()
    latest_file, datetime_abst = get_latest_file_and_date()
    date_abst = datetime.strptime(datetime_abst, '%Y%m%d')
    # to do: check if this is the date of currently active Abstimmung...
    if date_abst not in df_publ['abstimmungsdatum']:
        df_latest = make_df_for_publ(latest_file=latest_file, datetime_abst=datetime_abst)
        df_publ = pd.concat([df_latest, df_publ], ignore_index=True)

    df_viz = make_df_for_visualization(df=df_publ.copy(), datetime_abst=datetime_abst)
    # make date columns of string type
    df_publ['datum'] = df_publ['datum'].dt.strftime('%Y-%m-%d')
    df_publ['abstimmungsdatum'] = [str(x) for x in df_publ['abstimmungsdatum']]

    # upload csv files
    df_publ.to_csv(credentials.path_export_file_publ, index=False)
    df_viz.to_csv(credentials.path_export_file_viz, index=False)

    # push df_publ
    if ct.has_changed(credentials.path_export_file_publ):
        common.upload_ftp(credentials.path_export_file_publ, credentials.ftp_server, credentials.ftp_user,
                          credentials.ftp_pass, 'staka-abstimmungen')
        ct.update_hash_file(credentials.path_export_file_publ)
        logging.info("push data to ODS realtime API")
        logging.info("push for dataset 100223")
        push_url = credentials.ods_live_realtime_push_url_publ
        push_key = credentials.ods_live_realtime_push_key_publ
        common.ods_realtime_push_df(df_publ, url=push_url, push_key=push_key)
    # push df_viz
    if ct.has_changed(credentials.path_export_file_viz):
        common.upload_ftp(credentials.path_export_file_viz, credentials.ftp_server, credentials.ftp_user,
                          credentials.ftp_pass, 'staka-abstimmungen')
        ct.update_hash_file(credentials.path_export_file_viz)
        logging.info("push data to ODS realtime API")
        logging.info("push for dataset 100224")
        push_url = credentials.ods_live_realtime_push_url_viz
        push_key = credentials.ods_live_realtime_push_key_viz
        common.ods_realtime_push_df(df_viz, url=push_url, push_key=push_key)

def get_previous_data_from_20210307():
    pattern = '????????_Eingang_Stimmabgaben*morgen.xlsx'
    # to do: change the path
    file_list = glob.glob(os.path.join(credentials.path_local, pattern))
    df_all = pd.DataFrame()
    for file in file_list:
        datetime_abst = os.path.basename(file).split("_", 1)[0]
        print(datetime_abst)
        datetime_abst = datetime.strptime(datetime_abst, '%Y%m%d')
        df = make_df_for_publ(latest_file=file, datetime_abst=datetime_abst)
        df_all = pd.concat([df_all, df], ignore_index=True)
    return df_all


def get_latest_file_and_date():
    pattern = '????????_Eingang_Stimmabgaben*.xlsx'
    data_file_names = []
    # to do: change the path
    file_list = glob.glob(os.path.join(credentials.path_local, pattern))
    if len(file_list) > 0:
        latest_file = max(file_list, key=os.path.getmtime)
        data_file_names.append(os.path.basename(latest_file))
    datetime_abst = data_file_names[0].split("_", 1)[0]
    datetime_abst = datetime.strptime(datetime_abst, '%Y%m%d')
    return latest_file, datetime_abst


def make_df_for_publ(latest_file, datetime_abst):
    columns = ['tag', 'datum', 'eingang_pro_tag', 'eingang_kumuliert', 'stimmbeteiligung']
    dtypes = {'datum': 'datetime64'
     }
    df_stimmabgaben = pd.read_excel(latest_file,
                                    sheet_name=0,
                                    header=None,
                                    names=columns,
                                    skiprows=6,
                                    dtype=dtypes
                                    )
    df_stimmabgaben['stimmbeteiligung'] = 100 * df_stimmabgaben['stimmbeteiligung']
    # add column Abstimmungsdatum
    df_stimmabgaben["abstimmungsdatum"] = datetime_abst#.date()
    # remove empty rows
    df_stimmabgaben = df_stimmabgaben.dropna()
    return df_stimmabgaben


def make_df_for_visualization(df, datetime_abst):
    # df['tage_bis_abst'] = [(datetime_abst - d0).days for d0 in df['datum']]
    df['tage_bis_abst'] = df['abstimmungsdatum'] - df['datum']
    df['tage_bis_abst'] = [x.days for x in df['tage_bis_abst']]
    df['stimmbeteiligung_vis'] = [round(x, 1) if not np.isnan(x) else 0.0 for x in
                                               df['stimmbeteiligung']]
    df_stimmabgaben_vis = pd.DataFrame()
    df_stimmabgaben_vis[['datum', 'stimmbeteiligung', 'abstimmungsdatum']] = df[['datum', 'stimmbeteiligung_vis', 'abstimmungsdatum']]
    df_stimmabgaben_vis = df_stimmabgaben_vis[df.tage_bis_abst.isin([18, 11, 6, 5, 4, 3, 2, 1])]
    # add date of Abstimmung
    s = pd.DataFrame([[datetime_abst, 0.0, datetime_abst.date()]], columns=['datum', 'stimmbeteiligung', 'abstimmungsdatum'])
    df_stimmabgaben_vis = pd.concat([df_stimmabgaben_vis, s])
    df_stimmabgaben_vis['datum'] = df_stimmabgaben_vis['datum'].dt.strftime('%Y-%m-%d')
    df_stimmabgaben_vis['abstimmungsdatum'] = [str(x) for x in df_stimmabgaben_vis['abstimmungsdatum']]
    return df_stimmabgaben_vis


# Realtime API bootstrap data df_publ:
# {
# "tag": "Mittwoch",
#      "datum": "2022-05-15",
#      "eingang_pro_tag" : 1,
#      "eingang_kumuliert" : 1,
#     "stimmbeteiligung": 1.0,
#      "abstimmungsdatum": "2022-05-15"
# }

# Realtime API bootstrap data df_vis:
# {
#     "datum": "2022-05-15",
#     "stimmbeteiligung": 1.0,
#     "abstimmungsdatum": "2022-05-15"
# }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()