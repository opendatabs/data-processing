import pandas as pd
import json
import common
import urllib3
from lufthygiene_pm25 import credentials

url = 'https://data-bs.ch/lufthygiene/regionales-mikroklima/airmet_bs_sensirion_pm25_aktuell.csv'
print(f'Downloading data from {url}...')
df = common.pandas_read_csv(url, sep=';', encoding='cp1252', skiprows=range(1, 6))
print(f'Calculating ISO8601 time string...')
df['timestamp'] = pd.to_datetime(df.Zeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')

print(f'Reading latest timestamp from ODS dataset...')
urllib3.disable_warnings()
r = common.requests_get('https://data.bs.ch/api/records/1.0/search/?dataset=100081&q=&rows=1&sort=zeitstempel', verify=False)
r.raise_for_status()
latest_ods_timestamp = r.json()['records'][0]['fields']['zeitstempel']
print(f'Latest timestamp in ODS is {latest_ods_timestamp}.')
print(f'Latest timestamp in retrieved new data is {df.timestamp.max().strftime("%d.%m.%Y %H:%M:%S")}')
print(f'Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...')
realtime_df = df[df['timestamp'] > latest_ods_timestamp]

if len(realtime_df) == 0:
    print(f'No rows to push to ODS... ')
else:
    print(f'Melting dataframe...')
    ldf = realtime_df.melt(id_vars=['Zeit', 'timestamp'], var_name='station', value_name='pm_2_5')
    print(f'Dropping rows with empty pm25 value...')
    ldf = ldf.dropna(subset=['pm_2_5'])

    print(f'Pushing {ldf.timestamp.count()} rows to ODS realtime API...')
    # Realtime API bootstrap data:
    # Zeit,timestamp,station,pm_2_5
    # {
    #     "Zeit": "13.08.2020 00:30:00",
    #     "timestamp": "2020-08-12T22:30:00+00:00",
    #     "station": "Feldbergstrasse",
    #     "pm_2_5": 8.8
    # }

    # Push all records in one HTTP post
    ldf.timestamp = ldf.timestamp.dt.strftime('%Y-%m-%d %H:%M:%S%z')
    payload = ldf.to_json(orient="records")
    print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
    # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
    r = common.requests_post(url=credentials.ods_live_push_api_url, data=payload, verify=False)
    r.raise_for_status()

print('Job successful!')
