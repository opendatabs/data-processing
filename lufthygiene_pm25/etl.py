import pandas as pd
import io
import json
import common
from lufthygiene_pm25 import credentials


def main():
    url = 'https://data-bs.ch/lufthygiene/regionales-mikroklima/airmet_bs_sensirion_pm25_aktuell.csv'
    print(f'Downloading data from {url}...')
    r = common.requests_get(url)
    r.raise_for_status()
    s = r.text
    df = pd.read_csv(io.StringIO(s), sep=';', encoding='cp1252', skiprows=range(1, 2))
    print(f'Calculating ISO8601 time string...')
    df['timestamp'] = pd.to_datetime(df.Zeit, format='%d.%m.%Y %H:%M:%S').dt.tz_localize('Europe/Zurich', ambiguous=True, nonexistent='shift_forward')
    # Rename new St.Johann sensor back to old name
    df.rename(columns={'bl_StJohann2': 'St.Johann'}, inplace=True)

    # we simplify the code and re-push all current data all the time instead of checking for the latest timestamp in ODS.
    # Otherwise we'd need to check for the latest timestamp of each single sensor, instead of the latest overall.

    # print(f'Reading latest timestamp from ODS dataset...')
    # urllib3.disable_warnings()
    # r = common.requests_get('https://data.bs.ch/api/records/1.0/search/?dataset=100081&q=&rows=1&sort=zeitstempel', verify=False)
    # r.raise_for_status()
    # latest_ods_timestamp = r.json()['records'][0]['fields']['zeitstempel']
    # print(f'Latest timestamp retrieved from ODS is {latest_ods_timestamp}.')
    # print(f'Latest timestamp retrieved new data is {df.timestamp.max().strftime("%Y.%m.%dT%H:%M:%S%z")}')
    # print(f'Filtering data after {latest_ods_timestamp} for submission to ODS via realtime API...')
    # realtime_df = df[df['timestamp'] > latest_ods_timestamp]

    realtime_df = df
    if len(realtime_df) == 0:
        print(f'No rows to push to ODS... ')
    else:
        print(f'Melting dataframe...')
        ldf = realtime_df.melt(id_vars=['Zeit', 'timestamp'], var_name='station', value_name='pm_2_5')
        print(f'Dropping rows with empty pm25 value...')
        ldf = ldf.dropna(subset=['pm_2_5'])
        row_count = ldf.timestamp.count()
        if row_count == 0:
            print(f'No rows to push to ODS... ')
        else:
            print(f'Pushing {row_count} rows to ODS realtime API...')
            # Realtime API bootstrap data:
            # Zeit,timestamp,station,pm_2_5
            # {
            #     "Zeit": "13.08.2020 00:30:00",
            #     "timestamp": "2020-08-12T22:30:00+00:00",
            #     "station": "Feldbergstrasse",
            #     "pm_2_5": 8.8
            # }

            ldf.timestamp = ldf.timestamp.dt.strftime('%Y-%m-%d %H:%M:%S%z')
            payload = ldf.to_json(orient="records")
            # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
            # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
            r = common.requests_post(url=credentials.ods_live_push_api_url, data=payload, verify=False)
            r.raise_for_status()

    print('Job successful!')


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
