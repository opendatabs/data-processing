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

print(f'Executing calculations...')
pysqldf = lambda q: sqldf(q, globals())
# sum type 1 and 99, filter by BS, count distinct persons
df_bs_by = sqldf('select vacc_date, vacc_count, case reporting_unit_location_type when 1 then "vaccination_centre" when 99 then "vaccination_centre" when 6 then "hospital" else reporting_unit_location_type end as location_type, count(distinct person_anonymised_id) as count from df where reporting_unit_location_ctn = "BS" group by vacc_date, vacc_count, location_type;')
# https://stackoverflow.com/questions/43617871/pandas-dataframe-transpose-multi-columns
# df_pivot = df_bs_by.pivot(index=['vacc_date', 'vacc_count'], columns=['location_type'], values=['count']).reset_index()
# https://pandas.pydata.org/docs/user_guide/reshaping.html
# df_crosstab = pd.crosstab(index=df_bs_by.vacc_date, columns=[df_bs_by.vacc_count, df_bs_by.location_type], values=[df_bs_by.count], dropna=False)
# df_crosstab = pd.crosstab(index=df_bs_by.vacc_date, columns=[df_bs_by.vacc_count], values=[df_bs_by.count] agg, dropna=False)




# df_bs = df[df['reporting_unit_location_ctn']=='BS']
# # see https://medium.com/jbennetcodes/how-to-rewrite-your-sql-queries-in-pandas-and-more-149d341fc53e
# df_by = df_bs.groupby(['vacc_date', 'vacc_count', 'reporting_unit_location_type']).size().to_frame('count').reset_index()

bs_by_file = os.path.join(credentials.vmdl_path, f'vmdl_bs_by.csv')
print(f'Exporting resulting data to {bs_by_file}...')
df_bs_by.to_csv(bs_by_file, index=False)

print(f'Job successful!')