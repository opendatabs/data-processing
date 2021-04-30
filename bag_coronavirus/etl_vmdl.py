import pandas as pd
import requests
import os
import common
import pandas as pd
from io import StringIO
from pandasql import sqldf
from bag_coronavirus import credentials

payload_token = f'client_id={credentials.vmdl_client_id}&scope={credentials.vmdl_scope}&username={credentials.vmdl_user}&password={credentials.vmdl_password}&grant_type=password'
headers_token = {'Content-Type': 'application/x-www-form-urlencoded'}
print(f'Getting OAUTH2 access token...')
resp_token = requests.request("POST", credentials.vmdl_url_token, headers=headers_token, data=payload_token)
resp_token.raise_for_status()
# token_type = resp_token.json()['token_type']
auth_string = f'Bearer {resp_token.json()["access_token"]}'

payload_download={}
headers_download = {'Authorization': auth_string}
print(f'Downloading data...')
resp_download = common.requests_get(credentials.vmdl_url_download, headers=headers_download, data=payload_download)
# resp_download = requests.request("GET", credentials.vmdl_url_download, headers=headers_download, data=payload_download)
resp_download.raise_for_status()
# print(f'Reading data into StringIO...')
# vmdl_text = StringIO(resp_download.content)
file_path = os.path.join(credentials.vmdl_path, credentials.vmdl_file)
print(f'Writing data to file {file_path}...')
resp_download.encoding = 'utf-8'
with open(file_path, "w") as f:
    f.write(resp_download.text)

print(f'Reading data into dataframe...')
df = pd.read_csv(file_path, sep=';')
df['vacc_date_dt'] = pd.to_datetime(df.vacc_date, format='%Y-%m-%dT%H:%M:%S.%f%z')
df['vacc_day'] = df.vacc_date.str.slice(stop=10)

print(f'Executing calculations...')
pysqldf = lambda q: sqldf(q, globals())
# sum type 1 and 99, filter by BS, count distinct persons
df_bs = sqldf('select * from df where reporting_unit_location_ctn = "BS"')
df_bs_by = sqldf('select vacc_day, vacc_count, case reporting_unit_location_type when 1 then "vacc_centre" when 99 then "vacc_centre" when 6 then "hosp" else reporting_unit_location_type end as location_type, count(distinct person_anonymised_id) as count from df_bs group by vacc_day, vacc_count, location_type order by vacc_day asc;')

df_pivot = df_bs_by.pivot_table(values='count', index=['vacc_day'], columns=['location_type', 'vacc_count'])
# Replace the 2-level column names with a string that concatenates both strings
df_pivot.columns = ["_".join(str(c) for c in col) for col in df_pivot.columns.values]
df_pivot = df_pivot.reset_index()

df_pivot['hosp'] = df_pivot.hosp_1 + df_pivot.hosp_2
df_pivot['vacc_centre'] = df_pivot.vacc_centre_1 + df_pivot.vacc_centre_2
df_pivot['vacc_count_1'] = df_pivot.hosp_1 + df_pivot.vacc_centre_1
df_pivot['vacc_count_2'] = df_pivot.hosp_2 + df_pivot.vacc_centre_2

df_pivot['cum_1'] = df_pivot.vacc_count_1.cumsum()
df_pivot['cum_2'] = df_pivot.vacc_count_2.cumsum()
df_pivot['only_1'] = df_pivot.cum_1 - df_pivot.cum_2

df_total = sqldf('select vacc_day, count(distinct person_anonymised_id) as total from df_bs group by vacc_day order by vacc_day asc;')
df_total['total_cum'] = df_total.total.cumsum()

df_merged = df_pivot.merge(right=df_total, how='outer', on='vacc_day')

bs_by_file = os.path.join(credentials.vmdl_path, f'vmdl_bs_by.csv')
print(f'Exporting resulting data to {bs_by_file}...')
df_merged.to_csv(bs_by_file, index=False)

print(f'Job successful!')