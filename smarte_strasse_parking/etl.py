import datetime
import logging
import pandas as pd
from requests.auth import HTTPBasicAuth
import common
from smarte_strasse_parking import credentials


def main():
    df1 = push_current_state_data()
    # df2 = push_historical_data()
    pass


def push_current_state_data():
    logging.info(f'Retrieving current state data from API...')
    r = common.requests_get(credentials.api1_url, auth=HTTPBasicAuth(credentials.api1_user, credentials.api1_pw))
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(r.json(), record_path='attributes', meta=['id', 'type'], sep='_')
    df['id'] = df.id.astype(int)
    df_curr = df[['id', 'value_started', 'value_updated', 'value_received', 'value_label']]
    row_count = len(df_curr)
    if row_count == 0:
        print(f'No rows to push to ODS... ')
    else:
        print(f'Pushing {row_count} rows to ODS realtime API...')
        # Realtime API bootstrap data:
        # {
        #     "id": 0,
        #     "value_started": "2022-01-14T07:27:56.000Z",
        #     "value_updated": "2022-01-14T08:44:56.000Z",
        #     "value_received": "2022-01-14T08:44:56.000Z",
        #     "value_label": "1"
        # }
        payload = df_curr.to_json(orient="records")
        # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
        r = common.requests_post(url=credentials.ods_realtime_push_url_curr, data=payload, params={'pushkey': credentials.ods_realtime_push_key, 'apikey': credentials.ods_api_key})
        r.raise_for_status()
    return df_curr


def push_historical_data():
    logging.info(f'Retrieving historical data from API...')

    now = datetime.datetime.now()
    end = now.strftime('%Y%m%d%H%M')
    start = (now - datetime.timedelta(days=7)).strftime('%Y%m%d%H%M')
    headers = {'Authorization': f'Bearer {credentials.api2_token}'}
    r = common.requests_get(credentials.api2_url, params={'timezone': 'Europe/Zurich', 'starts': start, 'ends': end}, headers=headers)
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(r.json(), record_path='data')
    df['id'] = df.name.astype(int)
    df_hist = df[['id', 'fromDate', 'toDate']]
    row_count = len(df_hist)
    if row_count == 0:
        print(f'No rows to push to ODS... ')
    else:
        print(f'Pushing {row_count} rows to ODS realtime API...')
        # Realtime API bootstrap data:
        # {
        #     "id": 0,
        #     "fromDate": "2022-01-11T15:06:54+01:00",
        #     "toDate": "2022-01-11T15:28:54+01:00"
        # }
        payload = df_hist.to_json(orient="records")
        # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
        r = common.requests_post(url=credentials.ods_realtime_push_url_hist, data=payload, params={'pushkey': credentials.ods_realtime_push_key, 'apikey': credentials.ods_api_key})
        r.raise_for_status()
    return df_hist


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
