from bag_coronavirus_tests import credentials
import os
import common
import pandas as pd

print(f"Getting today's data url...")
context_json = common.requests_get(url='https://www.covid19.admin.ch/api/data/context').json()
hosp_url = context_json['sources']['individual']['csv']['daily']['hospCapacity']
print(f'Reading current csv into data frame...')
df = common.pandas_read_csv(hosp_url)
print(f'Keeping only non-forward-propagated data...')
df = df[df.type_variant == 'nfp']
print(f'Calculating columns...')
df.date = pd.to_datetime(df['date'])
df['dayofweek'] = df.date.dt.dayofweek
df['woche'] = df.date.dt.isocalendar().week
export_file_name = os.path.join(credentials.path, 'covid19_hosp_capacity.csv')
print(f'Exporting to file {export_file_name}...')
df.to_csv(export_file_name, index=False)
common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag')
print(f'Job successful!')

