from bag_coronavirus_tests import credentials
import os
import common
import glob
import requests
import pandas as pd

# search_string = os.path.join(credentials.path, '?????? CovidTests_BS.xlsx')
# print(f'Searching for files matching "{search_string}"...')
# files = glob.glob(search_string)
# print(f'Found {len(files)} matching files. Uploading to FTP server...')
# for file in sorted(files):
#     common.upload_ftp(os.path.join(credentials.path, file), credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag_coronavirus_tests')

print(f"Getting today's data url...")
context_json = requests.get('https://www.covid19.admin.ch/api/data/context').json()
csv_daily_tests_url = context_json['sources']['individual']['csv']['daily']['test']
print(f'Reading current csv into data frame...')
df = pd.read_csv(csv_daily_tests_url)
print(f'Filtering out BS rows, some columns, and rename them...')
df_bs = df.query('geoRegion == "BS"')
df_bs = df_bs.filter(items=['datum', 'entries_neg', 'entries_pos', 'entries'])
df_bs['positivity_rate_percent'] = df_bs['entries_pos'] / df_bs['entries'] * 100
df_bs['positivity_rate'] = df_bs['entries_pos'] / df_bs['entries']
df_bs = df_bs.rename(columns={'entries_neg': 'negative_tests', 'entries_pos': 'positive_tests', 'entries': 'total_tests'})
print(f'Calculating columns...')
df_bs['dayofweek'] = pd.to_datetime(df_bs['datum']).dt.dayofweek + 1
df_bs['woche'] = pd.to_datetime(df_bs['datum']).dt.week
export_file_name = os.path.join(credentials.path, credentials.file_name)
print(f'Exporting to file {export_file_name}...')
df_bs.to_csv(export_file_name, index=False)
print(f'Uploading to FTP...')
common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag_coronavirus_tests')

print(f'Job successful!')
