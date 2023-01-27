import pandas as pd
import common
from requests.auth import HTTPBasicAuth
from bafu_hydrodaten import credentials
import re
from datetime import datetime


rivers = ['Rhein', 'Birs']
methods = ['COSMO-1E ctrl', 'COSMO-2E ctrl', 'IFS']


def get_date_time(line):
    match = re.search(r'\d{1,2}.\d{1,2}.\d{4}, \d{2}.\d{2}', line)
    date_time = datetime.strptime(match.group(), '%d.%m.%Y, %H.%M')
    return date_time


def extract_data(river, method):
    url = credentials.dict_url[river][method]
    req = common.requests_get(url, auth=HTTPBasicAuth(credentials.https_user, credentials.https_pass))
    lines = req.content.splitlines()
    ausgabe_info = str(lines[6])
    ausgabe = get_date_time(ausgabe_info)
    meteolauf_info = str(lines[7])
    meteolauf = get_date_time(meteolauf_info)
    gemessen_info = str(lines[8])
    gemessen = get_date_time(gemessen_info)
    with open(f'det_{method}_{river}_table.txt', mode='wb') as file:
        for line in lines[14::]:
            file.write(line)
            file.write(b'\n')
    df = pd.read_table(f'det_{method}_{river}_table.txt', delim_whitespace=True)
    df['methode'] = method
    df['ausgegeben_an'] = ausgabe
    df['meteolauf'] = meteolauf
    df['gemessene_werten_bis'] = gemessen
    return df


river = 'Rhein'
df = pd.DataFrame()
for method in methods:
    df_method = extract_data(river, method)
    df = pd.concat([df, df_method])

for column in ['hh', 'dd', 'mm']:
    df[column] = [x if len(x) == 2 else ("0" + x) for x in df[column].astype(str)]
df['timestamp'] = df['mm'].astype(str) + '-' + df['dd'].astype(str) + '-' + df['yyyy'].astype(str) + ' ' + df['hh'].astype(str)
df.to_csv("det_rhein.csv", index=False)
