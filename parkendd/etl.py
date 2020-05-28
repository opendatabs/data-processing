import requests
import json
from datetime import datetime
from parkendd import credentials
import pandas as pd
import common

apiUrl = 'https://api.parkendd.de/Basel'
print(f'Getting latest data from {apiUrl}...')
response = requests.get(apiUrl)

json_file_name = f'{credentials.path}json/parkendd-{str(datetime.now()).replace(":", "")}.json'
print(f'Parsing json and saving to {json_file_name}...')
parsed = json.loads(response.text)
#print(response.json())
pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
#print(pretty_resp)
resp_file = open(json_file_name, 'w+')
resp_file.write(pretty_resp)
resp_file.close()

csv_file_name = f'{credentials.path}csv/parkendd-{str(datetime.now()).replace(":", "")}.csv'
print(f'Processing data...')
for lot in parsed['lots']:
    lot['last_downloaded'] = parsed['last_downloaded']
    lot['last_updated'] = parsed['last_updated']

normalized = pd.json_normalize(parsed, record_path='lots')
normalized['title'] = "Parkhaus " + normalized['name']
normalized['id2'] = normalized['id'].str.replace('baselparkhaus', '')
normalized['link'] = "https://www.parkleitsystem-basel.ch/parkhaus/" + normalized['id2']
normalized['description'] = 'Anzahl freie Parkplätze: ' + normalized['free'].astype(str)
normalized['published'] = normalized['last_downloaded']

print(f'Saving as {csv_file_name}...')
normalized.to_csv(csv_file_name, index=False)

print('Uploading json and csv file to ftp server...')
common.upload_ftp(csv_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parkendd/csv')
common.upload_ftp(json_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parkendd/json')

print('Job successful!')

