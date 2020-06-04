from shutil import copy2
import pandas as pd
import common
from ftplib import FTP
import requests
from verkehrszaehldaten import credentials
import sys
import os


def parse_truncate(path, filename, no_file_copy):
    generated_filenames = []
    if no_file_copy is False:
        path_to_file = os.path.join(path, filename)
        print(f"Copying file {path_to_file} to local directory...")
        copy2(path_to_file, filename)
    # Parse, process, truncate and write csv file
    print("Reading file " + filename + "...")
    data = pd.read_csv(filename,
                       engine='python',
                       sep=';',
                       # encoding='ANSI',
                       encoding='cp1252',
                       dtype={'SiteCode': 'category', 'SiteName': 'category', 'DirectionName': 'category', 'LaneName': 'category', 'TrafficType': 'category'})
    print("Processing " + filename + "...")
    data['DateTimeFrom'] = pd.to_datetime(data['Date'] + ' ' + data['TimeFrom'], format='%d.%m.%Y %H:%M')
    data['DateTimeTo'] = pd.to_datetime(data['Date'] + ' ' + data['TimeTo'], format='%d.%m.%Y %H:%M')
    data['Year'] = data['DateTimeFrom'].dt.year
    data['Month'] = data['DateTimeFrom'].dt.month
    data['Day'] = data['DateTimeFrom'].dt.day
    data['Weekday'] = data['DateTimeFrom'].dt.weekday
    data['HourFrom'] = data['DateTimeFrom'].dt.hour
    # Convert Datetime to GMT / UTC to simplify opendatasoft import
    # todo: Fix - does still not work for all dates
    data['DateTimeFrom'] = (data['DateTimeFrom'] - pd.Timedelta(hours=1)).dt.tz_localize('UTC')
    data['DateTimeTo'] = (data['DateTimeTo'] - pd.Timedelta(hours=1)).dt.tz_localize('UTC')
    current_filename = 'converted_' + filename
    print("Saving " + current_filename + "...")
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
    current_filename = 'truncated_' + filename
    print('Creating dataset ' + current_filename + "...")
    # latest_year = pd.datetime.now().year
    latest_year = data['Year'].max()
    years = [latest_year, latest_year - 1]
    truncated_data = data[data.Year.isin(years)]
    print("Saving " + current_filename + "...")
    truncated_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    # Create a seaparate dataset per year
    all_years = data.Year.unique()
    for year in all_years:
        year_data = data[data.Year.eq(year)]
        current_filename = str(year) + '_' + filename
        print('Saving ' + current_filename + "...")
        year_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)

    print('Created the following files to forther processing: ' + str(generated_filenames))
    return generated_filenames


def upload_ftp(filename, server, user, password):
    # Upload files to FTP Server
    ftp = FTP(server)
    ftp.login(user, password)
    print("Uploading " + filename + " to FTP server...")
    with open(filename, 'rb') as f:
        ftp.storlines('STOR %s' % filename, f)
    ftp.quit()
    return


def publish_ods_dataset(dataset_uid, creds):
    print("Telling OpenDataSoft to reload dataset " + dataset_uid + '...')
    response = requests.put('https://basel-stadt.opendatasoft.com/api/management/v2/datasets/' + dataset_uid + '/publish', params={'apikey': creds.api_key}, proxies={'https': creds.proxy})
    if response.status_code == 200:
        print('ODS publish command successful.')
    else:
        print('Problem with OpenDataSoft Management API: ')
        print(response)


no_file_copy = False
if 'no_file_copy' in sys.argv:
    no_file_copy = True
    print('Proceeding without copying files...')
path_orig = credentials.path_orig

# filename_orig = ['small_MIV_Class_10_1.csv']
filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv']
ods_dataset_uids = ['da_koisz3', 'da_ob8g0d']

# Test
# data = parse_truncate(path_orig, filename_orig)[2]


# Upload processed and truncated data
for datafile in filename_orig:
    file_names = parse_truncate(path_orig, datafile, no_file_copy)
    if not no_file_copy:
        for file in file_names:
            common.upload_ftp(file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')


# Make OpenDataSoft reload data sources
if not no_file_copy:
    for datasetuid in ods_dataset_uids:
        publish_ods_dataset(datasetuid, credentials)


# Upload original unprocessed data
if not no_file_copy:
    for orig_file in filename_orig:
        # upload_ftp(orig_file, ftp_server, ftp_user, ftp_pass)
        common.upload_ftp(orig_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, '')




