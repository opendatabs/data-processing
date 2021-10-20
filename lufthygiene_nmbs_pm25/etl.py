import numpy
import pandas as pd
import json
import common
import urllib3
from lufthygiene_nmbs_pm25 import credentials


def main():
    url = 'https://data-bs.ch/lufthygiene/nmbs_pm25/airmet_bs_museum_pm25_aktuell.csv'
    print(f'Downloading data from {url}...')
    urllib3.disable_warnings()
    df = common.pandas_read_csv(url, sep=';', encoding='cp1252', skiprows=range(1, 2))
    print(f'Calculating ISO8601 time string...')
    df['timestamp'] = pd.to_datetime(df.Anfangszeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich',
                                                                                                ambiguous='infer',
                                                                                                nonexistent='shift_forward')

    # We simplify the code and re-push all current data all the time instead of checking for the latest timestamp in ODS.
    # print(f'Reading latest timestamp from ODS dataset...')
    # urllib3.disable_warnings()
    # r = common.requests_get('https://data.bs.ch/api/records/1.0/search/?dataset=100100&q=&rows=1&sort=anfangszeit', verify=False)
    # r.raise_for_status()
    # latest_ods_timestamp = r.json()['records'][0]['fields']['anfangszeit']
    # print(f'Latest timestamp is {latest_ods_timestamp}.')
    # print(f'Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...')
    # realtime_df = df[df['timestamp'] > latest_ods_timestamp]
    # print(f'Pushing {realtime_df.timestamp.count()} rows to ODS realtime API...')

    realtime_df = df

    if len(realtime_df) == 0:
        print(f'No rows to push to ODS... ')
    else:
        print(f'Dropping empty values...')
        realtime_df.PM25_Sensirion = realtime_df.PM25_Sensirion.replace(' ', numpy.nan)
        realtime_df = realtime_df.dropna(subset=['PM25_Sensirion'])
        row_count = realtime_df.Anfangszeit.count()
        if row_count == 0:
            print(f'No rows to push to ODS... ')
        else:
            # Realtime API bootstrap data:
            # {
            #     "anfangszeit": "23.02.2021 10:30:00",
            #     "pm25": 13.3
            # }
            payload = (realtime_df.rename(columns={'Anfangszeit': 'anfangszeit', 'PM25_Sensirion': 'pm25'})[['anfangszeit', 'pm25']]
                       .to_json(orient="records")
                       )
            print(f'Pushing {row_count} rows to ODS realtime API...')
            # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
            # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
            r = common.requests_post(url=credentials.ods_live_push_api_url, data=payload, verify=False)
            r.raise_for_status()

    print('Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
