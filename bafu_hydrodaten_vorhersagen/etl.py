import pandas as pd
import numpy as np
import common
import logging
import re
from datetime import datetime, timedelta
from pytz import timezone
import os
import ods_publish.etl_id as odsp
from common import change_tracking as ct
from requests.auth import HTTPBasicAuth
from bafu_hydrodaten_vorhersagen import credentials


rivers = ['Rhein', 'Birs']
methods = ['ICON-CH1-EPS', 'ICON-CH2-EPS', 'IFS']
dict_id = {
    'Rhein': '100271',
    'Birs': '100272'
}


def main():
    for river in rivers:
        logging.info(f'process data for {river}')
        df = pd.DataFrame()
        for method in methods:
            logging.info(f'process data for {method}')
            df_method = extract_data(river, method)
            df_method['timestamp'] = df_method['dd'].astype(str) + '.' + df_method['mm'].astype(str) + '.' + df_method['yyyy'].astype(str) \
                              + ' ' + df_method['hh'].astype(str)
            #  manually specify which datetime objects must be considered DT 
            # (https://stackoverflow.com/questions/36757981/python-pandas-tz-localize-ambiguoustimeerror-cannot-infer-dst-time-with-non-d)
            infer_dst = np.array([False] * df_method.shape[0]) # all False -> every row considered DT
            df_method['timestamp'] = pd.to_datetime(df_method.timestamp, format='%d.%m.%Y %H').dt.tz_localize('Europe/Zurich',
                                                                                                nonexistent='shift_forward', ambiguous=infer_dst)
            duplicate_index = [idx for idx, value in enumerate(df_method.timestamp.duplicated(keep='last')) if value]
            if duplicate_index:
                df_method['timestamp'] = [correct_dst_timezone(x) if idx != duplicate_index[0] else x for idx, x in
                               enumerate(df_method['timestamp'])]
            else:
                df_method['timestamp'] = [correct_dst_timezone(x) for x in df_method['timestamp']]
            df = pd.concat([df, df_method])
            df = df.reset_index(drop=True)
        logging.info(f'add timestamp with daylight saving time if needed')
        for column in ['hh', 'dd', 'mm']:
            df[column] = [x if len(x) == 2 else ("0" + x) for x in df[column].astype(str)]
        # Alle Zeitstempel sind immer in Winterzeit (UTC+1)

        logging.info(f'remove measured data and add once with method "gemessen"')
        df = take_out_measured_data(df)
        df = df.reset_index(drop=True)
        logging.info('define df_export and uplad to ftp')
        df_export = df[['timestamp', 'Wasserstand', 'Abfluss', 'methode',
                        'ausgegeben_an', 'meteolauf', 'gemessene_werten_bis',
                        'H_min', 'H_p25', 'H_p50', 'H_p75', 'H_max',
                        'Q_min', 'Q_p25', 'Q_p50', 'Q_p75', 'Q_max']]
        export_filename = os.path.join(os.path.dirname(__file__), 'data/vorhersagen/export', f'{river}_Vorhersagen.csv')
        df_export.to_csv(export_filename, index=False, sep=';')
        if ct.has_changed(export_filename):
            common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              'hydrodata.ch/data/vorhersagen')
            odsp.publish_ods_dataset_by_id(dict_id[river])
            ct.update_hash_file(export_filename)


def get_date_time(line):
    match = re.search(r'\d{1,2}.\d{1,2}.\d{4}, \d{2}.\d{2}', line)
    date_time = datetime.strptime(match.group(), '%d.%m.%Y, %H.%M')
    date_time = date_time.replace(tzinfo=timezone('Europe/Zurich'))
    date_time = correct_dst_timezone(date_time)
    return date_time


def correct_dst_timezone(timestamp):
    if timestamp.dst() == timedelta(hours=1):
        timestamp = timestamp + timedelta(hours=1)
    else:
        pass
    return timestamp


def extract_data(river, method):
    url = credentials.dict_url[river][method]
    req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
    lines = req.content.splitlines()
    ausgabe_info = str(lines[6])
    ausgabe = get_date_time(ausgabe_info)
    meteolauf_info = str(lines[7])
    meteolauf = get_date_time(meteolauf_info)
    gemessen_info = str(lines[8])
    gemessen = get_date_time(gemessen_info)
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    path_to_file = f'{curr_dir}/data/vorhersagen/latest_data/det_{method}_{river}_table.txt'
    with open(path_to_file, mode='wb') as file:
        for line in lines[14::]:
            file.write(line)
            file.write(b'\n')
    df = pd.read_table(path_to_file, sep='\s+')
    df['methode'] = method
    df['ausgegeben_an'] = ausgabe
    df['meteolauf'] = meteolauf
    df['gemessene_werten_bis'] = gemessen
    if 'ICON' in method:
        df_quant = get_quantiles(river, method, url)
        df = pd.merge(df, df_quant[['dd', 'mm', 'yyyy', 'hh',
                                    'H_min', 'H_p25', 'H_p50', 'H_p75', 'H_max',
                                    'Q_min', 'Q_p25', 'Q_p50', 'Q_p75', 'Q_max']],
                      on=['dd', 'mm', 'yyyy', 'hh'], how='left')
    return df


def get_quantiles(river, method, url):
    url = url.replace('_Ctrl', '')
    req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
    lines = req.content.splitlines()
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    path_to_file = f'{curr_dir}/data/vorhersagen/latest_data/quant_{method}_{river}_table.txt'
    with open(path_to_file, mode='wb') as file:
        for line in lines[14::]:
            file.write(line)
            file.write(b'\n')
    return pd.read_table(path_to_file, sep='\s+')


def take_out_measured_data(df):
    for idx in df.index:
        if df['timestamp'][idx] <= df['gemessene_werten_bis'][idx]:
            df.loc[idx, 'methode'] = 'gemessen'
        df = df.drop_duplicates(subset=['methode', 'timestamp'])
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
