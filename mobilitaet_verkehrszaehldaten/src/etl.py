import logging
from shutil import copy2
import pandas as pd
import common
from common import change_tracking as ct
from mobilitaet_verkehrszaehldaten import credentials
from mobilitaet_verkehrszaehldaten.src import dashboard_calc
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
                       dtype={'SiteCode': 'category', 'SiteName': 'category', 'DirectionName': 'category',
                              'LaneName': 'category', 'TrafficType': 'category'})
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

    logging.info(f'Creating json files for dashboard...')
    dashboard_calc.create_files_for_dashboard(data, filename, dest_path)

    print(f'Created the following files to further processing: {str(generated_filenames)}')
    return generated_filenames


def main():
    no_file_copy = False
    if 'no_file_copy' in sys.argv:
        no_file_copy = True
        print('Proceeding without copying files...')

    # For the velo view in the dashboard,
    # we also need to show the temperature and precipitation data
    dashboard_calc.download_weather_station_data(credentials.path_dest)

    filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv', 'MIV_Speed.csv']

    # Upload processed and truncated data
    for datafile in filename_orig:
        datafile_with_path = os.path.join(credentials.path_orig, datafile)
        if True or ct.has_changed(datafile_with_path):
            file_names = parse_truncate(credentials.path_orig, datafile, credentials.path_dest, no_file_copy)
            if not no_file_copy:
                for file in file_names:
                    common.upload_ftp(file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')
                    os.remove(file)
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
