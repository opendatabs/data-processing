from covid19_hosp import credentials
import common
import os
import pandas as pd
import re
import shutil
from datetime import datetime
from functools import reduce


def parse_data_file(file_id):
    file_name = os.path.join(credentials.export_path, credentials.hosp_data_files[file_id]['filename'])
    print(f'Reading file {file_name} into dataframe...')
    return pd.read_csv(file_name)


print(f'Starting processing python script {__file__}...')

filename = os.path.join(credentials.export_path, credentials.hosp_data_files[0]['filename'])
print(f'Creating file copy, then replacing "0" with empty string in raw csv file {filename}...')
shutil.copy2(filename, filename.replace('.csv', '_orig.csv'))
with open(filename, 'r') as f:
    raw_data = f.read()
# Replace 0 with '' when followed by comma or newline individually
replaced_data1 = re.sub(',0\n', ',\n', raw_data)
replaced_data2 = re.sub(',0,', ',,', replaced_data1)
with open(filename, 'w') as f:
    f.write(replaced_data2)

print(f'Counting number of hospitals with data...')
df0 = parse_data_file(0)
df = df0.copy()
# df['hospital_count'] = df.drop(columns=['Datum']).count(axis='columns')
df['hospital_count'] = df.count(axis='columns')
df['date'] = pd.to_datetime(df['Datum'], format='%d-%m-%Y', errors='coerce')

print(f'Counting sum of cases in hospitals...')
df0['current_hosp'] = df0.sum(axis=1, skipna=True, numeric_only=True)
print(f'Determining if all hospitals have reported their data...')
df0['hospital_count'] = df['hospital_count']
# Add 1 here: The number of columns with data is one bigger than the number of hospitals because of the date column
# Entries before a certain date are set to true for simplicity's sake (in the early days of the pandemic, not all hospitals had to report cases)
df0['data_from_all_hosp'] = (df['hospital_count'] >= credentials.target_hosp_count + 1) | (df['date'] < datetime.strptime(credentials.target_hosp_count_from_date, '%Y-%m-%d'))

df1 = parse_data_file(1)
df1['current_hosp_non_resident'] = df1[credentials.hosp_df1_total_non_resident_columns].sum(axis=1, skipna=True, numeric_only=True)
df1['current_hosp_resident'] = df1[credentials.hosp_df1_total_resident_columns]

df2 = parse_data_file(2)
df2['current_icu'] = df2[credentials.hosp_df2_total_ips_columns].sum(axis=1, skipna=True, numeric_only=True)

print(f'Merging datasets...')
dfs = [df0, df1, df2]
df_merged = reduce(lambda left,right: pd.merge(left, right, how='outer', on='Datum'), dfs)
print(f'Reformatting date...')
df_merged['date'] = pd.to_datetime(df_merged['Datum'], format='%d-%m-%Y', errors='coerce')
print(f'Filtering columns...')
df_public = df_merged[['date', 'current_hosp', 'current_hosp_resident', 'current_hosp_non_resident', 'current_icu', 'IMCU', 'Normalstation', 'data_from_all_hosp']]

export_filename = os.path.join(credentials.export_path,credentials.export_filename_hosp)
print(f'Exporting merged dataset to file {export_filename}...')
df_public.to_csv(export_filename, index=False)

common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')





