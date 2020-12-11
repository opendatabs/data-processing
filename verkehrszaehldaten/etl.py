from shutil import copy2
import pandas as pd
import common
from verkehrszaehldaten import credentials
import sys
import os


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
    data['DateTimeTo'] = pd.to_datetime(data['Date'] + ' ' + data['TimeTo'], format='%d.%m.%Y %H:%M')
    data['Year'] = data['DateTimeFrom'].dt.year
    data['Month'] = data['DateTimeFrom'].dt.month
    data['Day'] = data['DateTimeFrom'].dt.day
    data['Weekday'] = data['DateTimeFrom'].dt.weekday
    data['HourFrom'] = data['DateTimeFrom'].dt.hour
    data['DayOfYear'] = data['DateTimeFrom'].dt.dayofyear
    # Convert Datetime to GMT / UTC to simplify opendatasoft import
    # todo: Fix - does still not work for all dates
    data['DateTimeFrom'] = (data['DateTimeFrom'] - pd.Timedelta(hours=1)).dt.tz_localize('UTC')
    data['DateTimeTo'] = (data['DateTimeTo'] - pd.Timedelta(hours=1)).dt.tz_localize('UTC')
    current_filename = os.path.join(dest_path, 'converted_' + filename)
    print(f"Saving {current_filename}...")
    data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    # group by SiteName, get latest rows (data is already sorted by date and time) so that ODS limit
    # of 250K is not exceeded
    # print("Creating dataset truncated_" + filename + "...")
    # grouped_data = data.groupby('SiteName')
    # sliced_data = grouped_data.tail(249900 / grouped_data.ngroups)
    # print("Saving truncated_" + filename + "...")
    # sliced_data.to_csv('truncated_' + filename, sep=';', encoding='utf-8', index=False)
    # return ['converted_' + filename, 'truncated_' + filename]

    # Only keep latest two years of data
    current_filename = os.path.join(dest_path, 'truncated_' + filename)
    print(f'Creating dataset {current_filename}...')
    # latest_year = pd.datetime.now().year
    latest_year = data['Year'].max()
    years = [latest_year, latest_year - 1]
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

    print(f'Created the following files to further processing: {str(generated_filenames)}')
    return generated_filenames


no_file_copy = False
if 'no_file_copy' in sys.argv:
    no_file_copy = True
    print('Proceeding without copying files...')

filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv']
# ods_dataset_uids = ['da_koisz3', 'da_ob8g0d']

# Upload processed and truncated data
for datafile in filename_orig:
    file_names = parse_truncate(credentials.path_orig, datafile, credentials.path_dest, no_file_copy)
    if not no_file_copy:
        for file in file_names:
            common.upload_ftp(file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')

# Upload original unprocessed data
if not no_file_copy:
    for orig_file in filename_orig:
        path_to_file = os.path.join(credentials.path_dest, orig_file)
        common.upload_ftp(path_to_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')




