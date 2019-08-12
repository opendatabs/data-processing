from shutil import copy2
import pandas as pd
from ftplib import FTP
import requests
import credentials


def parse_truncate(path, filename):
    print("Copying file " + path + filename + " to local directory...")
    copy2(path + filename, filename)
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
    data['Year'] = data['DateTimeTo'].dt.year
    data['Month'] = data['DateTimeTo'].dt.month
    data['Day'] = data['DateTimeTo'].dt.day
    data['Weekday'] = data['DateTimeTo'].dt.weekday
    data['HourFrom'] = data['DateTimeFrom'].dt.hour
    # Convert Datetime to GMT / UTC to simplify opendatasoft import
    data['DateTimeFrom'] = (data['DateTimeFrom'] - pd.Timedelta(hours=1)).dt.tz_localize('UTC')
    data['DateTimeTo'] = (data['DateTimeTo'] - pd.Timedelta(hours=1)).dt.tz_localize('UTC')
    print("Saving converted_" + filename + "...")
    data.to_csv('converted_' + filename, sep=';', encoding='utf-8', index=False)

    # group by SiteName, get latest rows (data is already sorted by date and time) so that ODS limit
    # of 250K is not exceeded
    print("Creating dataset truncated_" + filename + "...")
    grouped_data = data.groupby('SiteName')
    sliced_data = grouped_data.tail(249900 / grouped_data.ngroups)
    print("Saving truncated_" + filename + "...")
    sliced_data.to_csv('truncated_' + filename, sep=';', encoding='utf-8', index=False)
    return ['converted_' + filename, 'truncated_' + filename]


def upload_ftp(filename, server, user, password):
    # Upload files to FTP Server
    ftp = FTP(server)
    ftp.login(user, password)
    print ("Uploading " + filename + " to FTP server...")
    with open(filename, 'rb') as f:
        ftp.storlines('STOR %s' % filename, f)
    ftp.quit()
    return


path_orig = credentials.path_orig

# filename_orig = ['small_MIV_Class_10_1.csv']
filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv']

ftp_server = credentials.ftp_server
ftp_user = credentials.ftp_user
ftp_pass = credentials.ftp_pass

# Test
# data = parse_truncate(path_orig, filename_orig)[2]


# Upload processed and truncated data
for datafile in filename_orig:
    file_names = parse_truncate(path_orig, datafile)
    for file in file_names:
        upload_ftp(file, ftp_server, ftp_user, ftp_pass)


# Upload original unprocessed data
for orig_file in filename_orig:
    upload_ftp(orig_file, ftp_server, ftp_user, ftp_pass)


# Make OpenDataSoft reload data source
print("Telling OpenDataSoft to reload dataset...")
response = requests.put('https://basel-stadt.opendatasoft.com/api/management/v2/datasets/da_koisz3/publish', params={'apikey': credentials.api_key}, proxies={'https': credentials.proxy})
if response.status_code == 200:
    print('Job Successful')
else:
    print('Problem with OpenDataSoft Management API: ')
    print(response)


