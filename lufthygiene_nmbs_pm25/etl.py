import pandas as pd
import common
import urllib3
from lufthygiene_nmbs_pm25 import credentials

url = 'https://data-bs.ch/lufthygiene/nmbs_pm25/airmet_bs_museum_pm25_aktuell.csv'
print(f'Downloading data from {url}...')
df = common.pandas_read_csv(url, sep=';', encoding='cp1252', skiprows=range(1, 2))
print(f'Calculating ISO8601 time string...')
df['timestamp'] = pd.to_datetime(df.Anfangszeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous='infer', nonexistent='shift_forward')

print(f'Reading latest timestamp from ODS dataset...')
urllib3.disable_warnings()

r = common.requests_get('https://data.bs.ch/api/records/1.0/search/?dataset=100100&q=&rows=1&sort=anfangszeit', verify=False)
r.raise_for_status()
latest_ods_timestamp = r.json()['records'][0]['fields']['anfangszeit']
print(f'Latest timestamp is {latest_ods_timestamp}.')
print(f'Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...')
realtime_df = df[df['timestamp'] > latest_ods_timestamp]
print(f'Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...')

for index, row in realtime_df.iterrows():
    timestamp_text = row.timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')
    # Realtime API bootstrap data:
    # {
    #     "anfangszeit": "23.02.2021 10:30:00",
    #     "pm25": 13.3
    # }
    payload = {"anfangszeit": row['Anfangszeit'],  "pm25": row['PM25_Sensirion']}
    print(f'Pushing row {index} with with the following data to ODS: {payload}')
    r = common.requests_post(url=credentials.ods_live_push_api_url, json=payload, verify=False)
    r.raise_for_status()

print('Job successful!')
