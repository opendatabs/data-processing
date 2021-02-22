import pandas as pd
import common
import urllib3
from lufthygiene_pm25 import credentials


@common.retry(common.http_errors_to_handle, tries=6, delay=10, backoff=1)
def read_data_from_url(uri):
    return pd.read_csv(uri, sep=';', encoding='cp1252', skiprows=range(1, 6))


url = 'https://data-bs.ch/lufthygiene/regionales-mikroklima/airmet_bs_sensirion_pm25_aktuell.csv'
print(f'Downloading data from {url}...')
df = read_data_from_url(url)
print(f'Calculating ISO8601 time string...')
df['timestamp'] = pd.to_datetime(df.Zeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous='infer')

print(f'Reading latest timestamp from ODS dataset...')
urllib3.disable_warnings()

r = common.requests_get('https://data.bs.ch/api/records/1.0/search/?dataset=100081&q=&rows=1&sort=zeitstempel', verify=False)
r.raise_for_status()
latest_ods_timestamp = r.json()['records'][0]['fields']['zeitstempel']
print(f'Latest timestamp is {latest_ods_timestamp}.')
print(f'Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...')
realtime_df = df[df['timestamp'] > latest_ods_timestamp]
print(f'Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...')

for index, row in realtime_df.iterrows():
    timestamp_text = row.timestamp.strftime('%Y-%m-%dT%H:%M:%S%z')
    # Realtime API bootstrap data:
    # {"Zeit": "19.02.2021 15:30:00",
    #  "Feldbergstrasse": "5.1",
    #  "St.Johann": "5.0",
    #  "BS Grenzacherstrasse 103": "4.4",
    #  "Hochbergerstrasse 162": "3.5",
    #  "Laufenstrasse 67": "5.3",
    #  "Rennweg 89": "5.2",
    #  "Zürcherstrasse 148": "5.2",
    #  "Erlenparkweg 55": "3.8",
    #  "Goldbachweg": "3.7",
    #  "Binningen": "5.2"
    #  }
    payload = {'Zeit': row.Zeit, 'Feldbergstrasse': row.Feldbergstrasse, 'St.Johann': row['St.Johann'], 'BS Grenzacherstrasse 103': row['BS Grenzacherstrasse 103'],
       'Hochbergerstrasse 162': row['Hochbergerstrasse 162'], 'Laufenstrasse 67': row['Laufenstrasse 67'], 'Rennweg 89': row['Rennweg 89'],
       'Zürcherstrasse 148': row['Zürcherstrasse 148'], 'Erlenparkweg 55': row['Erlenparkweg 55'], 'Goldbachweg': row['Goldbachweg'], 'Binningen': row['Binningen']}
    print(f'Pushing row {index} with with the following data to ODS: {payload}')
    r = common.requests_post(url=credentials.ods_live_push_api_url, json=payload, verify=False)
    r.raise_for_status()

print('Job successful!')
