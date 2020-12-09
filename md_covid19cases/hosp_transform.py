from md_covid19cases import credentials
import common
import os
import pandas as pd
from functools import reduce


def parse_data_file(file_id):
    filename = os.path.join(credentials.export_path, credentials.hosp_data_files[file_id]['filename'])
    print(f'reading file {filename} into dataframe...')
    return pd.read_csv(filename)


df0 = parse_data_file(0)
df0['current_hosp'] = df0.sum(axis=1, skipna=True, numeric_only=True)

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
df_public = df_merged[['date', 'current_hosp', 'current_hosp_resident', 'current_hosp_non_resident', 'current_icu']]

export_filename = os.path.join(credentials.export_path,credentials.export_filename_hosp)
print(f'Exporting merged dataset to file {export_filename}...')
df_public.to_csv(export_filename, index=False)

common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')





