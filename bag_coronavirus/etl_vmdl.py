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

print(f'Executing calculations...')
pysqldf = lambda q: sqldf(q, globals())
df_bs_by = sqldf('select vacc_date, vacc_count, reporting_unit_location_type, count(distinct person_anonymised_id) '
                'from df '
                'where reporting_unit_location_ctn = "BS" '
                'group by vacc_date, vacc_count, reporting_unit_location_type;')





# df_bs = df[df['reporting_unit_location_ctn']=='BS']
# # see https://medium.com/jbennetcodes/how-to-rewrite-your-sql-queries-in-pandas-and-more-149d341fc53e
# df_by = df.groupby(['vacc_date', 'vacc_count', 'reporting_unit_location_type']).size().to_frame('count').reset_index()

by_file = os.path.join(credentials.vmdl_path, f'vmdl_by.csv')
print(f'Exporting resulting data to {by_file}...')
df_bs_by.to_csv(by_file, index=False)

print(f'Job successful!')