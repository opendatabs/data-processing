import pandas as pd
import common
from requests.auth import HTTPBasicAuth
from bafu_hydrodaten import credentials

url = credentials.url_det_C1E_Rhein
req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
lines = req.content.splitlines()

with open('det_C1E_Rhein_table.txt', mode='wb') as file:
    for line in lines[14::]:
        file.write(line)
        file.write(b'\n')

df1 = pd.read_table('det_C1E_Rhein_table.txt', delim_whitespace=True)
df1['methode'] = 'C1E'
df1['ausgebeben_an'] = ''
df1['gemessen'] = ''

url = credentials.url_det_C2E_Rhein
req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
lines = req.content.splitlines()

with open('det_C2E_Rhein_table.txt', mode='wb') as file:
    for line in lines[14::]:
        file.write(line)
        file.write(b'\n')

df2 = pd.read_table('det_C2E_Rhein_table.txt', delim_whitespace=True)
df2['methode'] = 'C2E'
df2['ausgebeben_an'] = ''
df2['gemessen'] = ''

url = credentials.url_det_IFS_Rhein
req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
lines = req.content.splitlines()

with open('det_IFS_Rhein_table.txt', mode='wb') as file:
    for line in lines[14::]:
        file.write(line)
        file.write(b'\n')

df3 = pd.read_table('det_IFS_Rhein_table.txt', delim_whitespace=True)
df3['methode'] = 'IFS'
df3['ausgebeben_an'] = ''
df3['gemessen'] = ''



df = pd.concat([df1, df2, df3])
for column in ['hh', 'dd', 'mm']:
    df[column] = [x if len(x) == 2 else ("0" + x) for x in df[column].astype(str)]
df['timestamp'] = df['mm'].astype(str) + '-' + df['dd'].astype(str) + '-' + df['yyyy'].astype(str) + ' ' + df['hh'].astype(str)
df.to_csv("det_rhein.csv", index=False)
