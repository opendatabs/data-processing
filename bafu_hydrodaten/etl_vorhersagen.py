import pandas as pd
import common
from requests.auth import HTTPBasicAuth
from bafu_hydrodaten import credentials
import re
from datetime import datetime


date_patterns = [r'\d{2}.\d{1}.\d{4}, \d{2].\d{2}']

url = credentials.url_det_C1E_Rhein
req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
lines = req.content.splitlines()
info_C1E = lines[6:9]
ausgabe_info = str(lines[6])
match = re.search(r'\d{1,2}.\d{1,2}.\d{4}, \d{2}.\d{2}', ausgabe_info)
ausgabe = datetime.strptime(match.group(), '%d.%m.%Y, %H.%M')

with open('det_C1E_Rhein_table.txt', mode='wb') as file:
    for line in lines[14::]:
        file.write(line)
        file.write(b'\n')

df1 = pd.read_table('det_C1E_Rhein_table.txt', delim_whitespace=True)
df1['methode'] = 'COSMO-1E ctrl'
df1['ausgegeben_an'] = ausgabe

url = credentials.url_det_C2E_Rhein
req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
lines = req.content.splitlines()
info_C2E = lines[6:9]
ausgabe_info = str(lines[6])
match = re.search(r'\d{1,2}.\d{1,2}.\d{4}, \d{2}.\d{2}', ausgabe_info)
ausgabe = datetime.strptime(match.group(), '%d.%m.%Y, %H.%M')

with open('det_C2E_Rhein_table.txt', mode='wb') as file:
    for line in lines[14::]:
        file.write(line)
        file.write(b'\n')

df2 = pd.read_table('det_C2E_Rhein_table.txt', delim_whitespace=True)
df2['methode'] = 'COSMO-2E ctrl'
df2['ausgegeben_an'] = ausgabe


url = credentials.url_det_IFS_Rhein
req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
lines = req.content.splitlines()
info_IFS = lines[6:9]
ausgabe_info = str(lines[6])
match = re.search(r'\d{1,2}.\d{1,2}.\d{4}, \d{2}.\d{2}', ausgabe_info)
ausgabe = datetime.strptime(match.group(), '%d.%m.%Y, %H.%M')

with open('det_IFS_Rhein_table.txt', mode='wb') as file:
    for line in lines[14::]:
        file.write(line)
        file.write(b'\n')

df3 = pd.read_table('det_IFS_Rhein_table.txt', delim_whitespace=True)
df3['methode'] = 'IFS'
df3['ausgegeben_an'] = ausgabe


df = pd.concat([df1, df2, df3])
for column in ['hh', 'dd', 'mm']:
    df[column] = [x if len(x) == 2 else ("0" + x) for x in df[column].astype(str)]
df['timestamp'] = df['mm'].astype(str) + '-' + df['dd'].astype(str) + '-' + df['yyyy'].astype(str) + ' ' + df['hh'].astype(str)
df.to_csv("det_rhein.csv", index=False)
