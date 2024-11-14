import logging
from shutil import copy2
import pandas as pd
import common
from common import change_tracking as ct
from mobilitaet_verkehrszaehldaten import credentials
import sys
import os
import platform
import sqlite3
import pytz
import io

print(f'Python running on the following architecture:')
print(f'{platform.architecture()}')


def parse_truncate(path, filename, dest_path, no_file_cp):
    generated_filenames = []
    path_to_orig_file = os.path.join(path, filename)
    path_to_copied_file = os.path.join(dest_path, filename)
    if no_file_cp is False:
        print(f"Copying file {path_to_orig_file} to {path_to_copied_file}...")
        copy2(path_to_orig_file, path_to_copied_file)
    # Parse, process, truncate and write csv file
    print(f"Reading file {filename}...")
    data = pd.read_csv(path_to_copied_file,
                       engine='python',
                       sep=';',
                       # encoding='ANSI',
                       encoding='cp1252',
                       dtype={'SiteCode': 'category', 'SiteName': 'category', 'DirectionName': 'category', 'LaneName': 'category', 'TrafficType': 'category'})
    print(f"Processing {path_to_copied_file}...")
    data['DateTimeFrom'] = pd.to_datetime(data['Date'] + ' ' + data['TimeFrom'], format='%d.%m.%Y %H:%M')
    data['DateTimeTo'] = data['DateTimeFrom'] + pd.Timedelta(hours=1)
    data['Year'] = data['DateTimeFrom'].dt.year
    data['Month'] = data['DateTimeFrom'].dt.month
    data['Day'] = data['DateTimeFrom'].dt.day
    data['Weekday'] = data['DateTimeFrom'].dt.weekday
    data['HourFrom'] = data['DateTimeFrom'].dt.hour
    data['DayOfYear'] = data['DateTimeFrom'].dt.dayofyear
    print(f'Retrieving Zst_id as the first word in SiteName...')
    data['Zst_id'] = data['SiteName'].str.split().str[0]
    current_filename = os.path.join(dest_path, 'converted_' + filename)
    print(f"Saving {current_filename}...")
    data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    db_filename = os.path.join(dest_path, filename.replace('.csv', '.db'))
    print(f'Saving into sqlite db {db_filename}...')
    conn = sqlite3.connect(db_filename)
    data.to_sql(name=db_filename.split(os.sep)[-1].replace('.db', ''), con=conn, if_exists='replace', index=False)
    common.upload_ftp(db_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')

    # group by SiteName, get latest rows (data is already sorted by date and time) so that ODS limit
    # of 250K is not exceeded
    # print("Creating dataset truncated_" + filename + "...")
    # grouped_data = data.groupby('SiteName')
    # sliced_data = grouped_data.tail(249900 / grouped_data.ngroups)
    # print("Saving truncated_" + filename + "...")
    # sliced_data.to_csv('truncated_' + filename, sep=';', encoding='utf-8', index=False)
    # return ['converted_' + filename, 'truncated_' + filename]

    # Only keep latest n years of data
    keep_years = 2
    current_filename = os.path.join(dest_path, 'truncated_' + filename)
    print(f'Creating dataset {current_filename}...')
    latest_year = data['Year'].max()
    years = range(latest_year - keep_years, latest_year + 1)
    print(f'Keeping only data for the following years in the truncated file: {list(years)}...')
    truncated_data = data[data.Year.isin(years)]
    print(f"Saving {current_filename}...")
    truncated_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    # Create a separate dataset per year
    all_years = data.Year.unique()
    for year in all_years:
        year_data = data[data.Year.eq(year)]
        current_filename = os.path.join(dest_path, str(year) + '_' + filename)
        print(f'Saving {current_filename}...')
        year_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)
        
    if 'MIV_Speed' not in filename:
        # Create a separate dataset per ZST_NR
        all_sites = data.Zst_id.unique()
        for site in all_sites:
            for traffic_type in ['MIV', 'Velo', 'Fussgänger']:
                site_data = data[data.Zst_id.eq(site) & data.TrafficType.eq(traffic_type)]
                if site_data.empty:
                    continue
                current_filename = os.path.join(dest_path, 'sites',
                                                'Fussgaenger' if traffic_type == 'Fussgänger' else traffic_type,
                                                f'{str(site)}.csv')
                print(f'Saving {current_filename}...')
                site_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
                generated_filenames.append(current_filename)

    df_dtv = calculate_dtv_zst(data, dest_path, filename)
    if df_dtv is not None:
        current_filename = os.path.join(dest_path, 'dtv_' + filename)
        print(f'Saving {current_filename}...')
        df_dtv.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)

    print(f'Created the following files to further processing: {str(generated_filenames)}')
    return generated_filenames


def calculate_dtv_zst(df, dest_path, filename):
    url_to_locations = 'https://data.bs.ch/explore/dataset/100038/download/'
    params = {
        'format': 'csv',
        'timezone': 'Europe/Zurich',
        'klasse': 'Dauerzaehlstelle'
    }
    r = common.requests_get(url_to_locations, params=params)
    df_locations = pd.read_csv(io.StringIO(r.text), sep=';', encoding='utf-8')
    # Expand ZWECK to several lines if there is a +
    df_locations['zweck'] = df_locations['zweck'].str.split('+')
    df_locations = df_locations.explode('zweck')
    # Replace Velo/Moto with Velo and Fuss with Fussgänger
    df_locations['zweck'] = df_locations['zweck'].str.replace('Velo/Moto', 'Velo')
    df_locations['zweck'] = df_locations['zweck'].str.replace('Fuss', 'Fussgänger')
    # Save id_zst as string
    df_locations['id_zst'] = df_locations['id_zst'].astype(str)

    # For each filename first sum up the daily traffic volume per site and traffic type, then calculate the average
    aggregation_dict = {
        'MIV_Speed.csv': ['Total', '20', '20_30', '30_40', '40_50', '50_60', '60_70', '70_80', '80_90', '90_100',
                          '100_110', '110_120', '120_130', '130'],
        'Velo_Fuss_Count.csv': ['Total'],
        'MIV_Class_10_1.csv': ['Total', 'MR', 'PW', 'PW+', 'Lief', 'Lief+', 'Lief+Aufl.', 'LW', 'LW+', 'Sattelzug',
                               'Bus', 'andere']
    }
    if filename in aggregation_dict:
        columns = aggregation_dict[filename]
        df_tv = df.groupby(['Zst_id', 'Date', 'TrafficType'])[columns].sum().reset_index()
        df_dtv = df_tv.groupby(['Zst_id', 'TrafficType'])[columns].mean().reset_index()

        df_count_data_points = df_tv.groupby(['Zst_id', 'TrafficType'])[['Total']].count().reset_index()
        df_dtv['Num_HourValues'] = df_count_data_points['Total']
        # Merge with locations
        df_dtv = df_dtv.merge(df_locations, left_on=['Zst_id', 'TrafficType'], right_on=['id_zst', 'zweck'], how='left')

        return df_dtv


def main():
    no_file_copy = False
    if 'no_file_copy' in sys.argv:
        no_file_copy = True
        print('Proceeding without copying files...')

    filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv', 'MIV_Speed.csv']

    # Upload processed and truncated data
    for datafile in filename_orig:
        datafile_with_path = os.path.join(credentials.path_orig, datafile)
        if True or ct.has_changed(datafile_with_path):
            file_names = parse_truncate(credentials.path_orig, datafile, credentials.path_dest, no_file_copy)
            if not no_file_copy:
                for file in file_names:
                    if ct.has_changed(file):
                        if 'sites' in file:
                            type = file.split(os.sep)[-2]
                            common.upload_ftp(file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                              f'verkehrszaehl_dashboard/data/{type}')
                        else:
                            common.upload_ftp(file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                                              '')
                        ct.update_hash_file(file)
            ct.update_hash_file(datafile_with_path)

    # Upload original unprocessed data
    if not no_file_copy:
        for orig_file in filename_orig:
            path_to_file = os.path.join(credentials.path_dest, orig_file)
            if ct.has_changed(path_to_file):
                common.upload_ftp(path_to_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')
                ct.update_hash_file(path_to_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
