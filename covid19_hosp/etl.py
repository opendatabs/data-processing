import logging
import re
import shutil
from datetime import datetime
from functools import reduce
import pandas as pd
from covid19_hosp import credentials
import common
import requests
import os
from bs4 import BeautifulSoup
from common import change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    extract()
    df_public, export_filename = transform()
    if ct.has_changed(export_filename):
        common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
        odsp.publish_ods_dataset_by_id('100109')
        ct.update_hash_file(export_filename)
    logging.info('Job successful!')


def extract():
    logging.info(f'Starting processing python script {__file__}...')
    # use session to retain cookie infos
    session = requests.Session()
    # extract necessary info from the login form
    login_form_url = credentials.hosp_domain + credentials.hosp_url_path
    logging.info(f'Getting content of login form at {login_form_url}...')
    resp_loginform = session.get(login_form_url, timeout=10)
    resp_loginform.raise_for_status()
    soup_login = BeautifulSoup(resp_loginform.content, 'html.parser')
    # logging.info(soup_login.prettify())
    action_url = soup_login.find('form').get('action')
    inputs = soup_login.find_all('input')
    token = soup_login.find_all(attrs={"name": "csrfmiddlewaretoken"})[0].get('value')
    next_url = soup_login.find_all(attrs={"name": "next"})[0].get('value')
    action_url = soup_login.find(id='login-form').get('action')
    # logging.info(f'Cookies: {resp_loginform.cookies}')
    login_form_action_url = credentials.hosp_domain + action_url
    logging.info(f'Posting login form to {login_form_action_url}...')
    payload = dict(username=credentials.hosp_username,
                   password=credentials.hosp_password,
                   csrfmiddlewaretoken=token,
                   next=next_url)
    req_spital_bs = session.post(login_form_action_url, data=payload, headers=dict(Referer=login_form_url), timeout=10)
    req_spital_bs.raise_for_status()
    soup_spital_bs = BeautifulSoup(req_spital_bs.content, 'html.parser')
    # logging.info(soup_spital_bs.prettify())
    for data_spec in credentials.hosp_data_files:
        logging.info(f'Retrieving data from widget {data_spec["widget_id"]}...')
        # data_from_html = soup_spital_bs.find(id=data_spec['id']).text
        data_from_html = soup_spital_bs.find_all(attrs={'widget_id': data_spec['widget_id']})[0].text
        # logging.info(f'{data_spec["id"]}: {data_from_html}')
        export_file_path = os.path.join(credentials.export_path, data_spec['filename'])
        logging.info(f'Saving data to file {export_file_path}...')
        f = open(export_file_path, 'w')
        f.write(data_from_html)
        f.close()


def parse_data_file(file_id):
    file_name = os.path.join(credentials.export_path, credentials.hosp_data_files[file_id]['filename'])
    logging.info(f'Reading file {file_name} into dataframe...')
    return pd.read_csv(file_name)


def transform():
    logging.info(f'Starting processing python script {__file__}...')

    filename = os.path.join(credentials.export_path, credentials.hosp_data_files[0]['filename'])
    logging.info(f'Creating file copy, then replacing "0" with empty string in raw csv file {filename}...')
    shutil.copy2(filename, filename.replace('.csv', '_orig.csv'))
    with open(filename, 'r') as f:
        raw_data = f.read()
    logging.info(f'Add newline at the end of the file, because without a newline at the end the last 0 in the file cannot be replaced by NULL afterwards...')
    newline_data = raw_data + '\n'
    logging.info(f'Replace 0 with "" when followed by comma or newline...')
    replaced_data1 = re.sub(',0\n', ',\n', newline_data)
    replaced_data2 = re.sub(',0,', ',,', replaced_data1)
    with open(filename, 'w') as f:
        f.write(replaced_data2)

    logging.info(f'Counting number of hospitals with data...')
    df0 = parse_data_file(0)
    df = df0.copy()
    # df['hospital_count'] = df.drop(columns=['Datum']).count(axis='columns')
    df['hospital_count'] = df.count(axis='columns')
    df['date'] = pd.to_datetime(df['Datum'], format='%d/%m/%Y')

    logging.info(f'Counting sum of cases in hospitals...')
    df0['current_hosp'] = df0.sum(axis=1, skipna=True, numeric_only=True)
    logging.info(f'Determining if all hospitals have reported their data...')
    df0['hospital_count'] = df['hospital_count']
    # Add 1 here: The number of columns with data is one bigger than the number of hospitals because of the date column
    # Entries before a certain date are set to true for simplicity's sake (in the early days of the pandemic, not all hospitals had to report cases)
    df0['data_from_all_hosp'] = (df['hospital_count'] >= credentials.target_hosp_count + 1) | (df['date'] < datetime.strptime(credentials.target_hosp_count_from_date, '%Y-%m-%d'))

    df1 = parse_data_file(1)
    df1['current_hosp_non_resident'] = df1[credentials.hosp_df1_total_non_resident_columns].sum(axis=1, skipna=True, numeric_only=True)
    df1['current_hosp_resident'] = df1[credentials.hosp_df1_total_resident_columns]

    df2 = parse_data_file(2)
    df2['current_icu'] = df2[credentials.hosp_df2_total_ips_columns].sum(axis=1, skipna=True, numeric_only=True)

    logging.info(f'Merging datasets...')
    dfs = [df0, df1, df2]
    df_merged = reduce(lambda left,right: pd.merge(left, right, how='outer', on='Datum'), dfs)
    logging.info(f'Reformatting date...')
    df_merged['date'] = pd.to_datetime(df_merged['Datum'], format='%d/%m/%Y')
    logging.info(f'Filtering columns...')
    df_public = df_merged[['date', 'current_hosp', 'current_hosp_resident', 'current_hosp_non_resident', 'current_icu', 'IMCU', 'Normalstation', 'data_from_all_hosp']]

    export_filename = os.path.join(credentials.export_path,credentials.export_filename_hosp)
    logging.info(f'Exporting merged dataset to file {export_filename}...')
    df_public.to_csv(export_filename, index=False)
    return df_public, export_filename


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
