from bag_coronavirus import credentials
import os
import common
import pandas as pd

print(f"Getting today's data url...")
context_json = common.requests_get(url='https://www.covid19.admin.ch/api/data/context').json()
csv_daily_tests_url = context_json['sources']['individual']['csv']['daily']['testPcrAntigen']
print(f'Reading current csv into data frame...')
df = common.pandas_read_csv(csv_daily_tests_url)
print(f'Calculating columns...')
df['dayofweek'] = pd.to_datetime(df['datum']).dt.dayofweek
df['wochentag'] = df['dayofweek'].apply(lambda x: common.weekdays_german[x])
df['week'] = pd.to_datetime(df['datum']).dt.week
export_file_name = os.path.join(credentials.path, 'covid19_testPcrAntigen.csv')
print(f'Exporting to file {export_file_name}...')
df.to_csv(export_file_name, index=False)
common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag')
print(f'Job successful!')
