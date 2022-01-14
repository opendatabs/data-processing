import logging

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import common
from smarte_strasse_parking import credentials


def main():
    df2 = retrieve_historical_data()

    df1 = retrieve_current_state_data()
    df_curr = df1[['id', 'value_started', 'value_updated', 'value_received', 'value_label']]
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
        r = common.requests_post(url=credentials.ods_realtime_push_url, data=payload, params={'pushkey': credentials.ods_realtime_push_key, 'apikey': credentials.ods_api_key})
        r.raise_for_status()
        pass

    pass


def retrieve_current_state_data():
    logging.info(f'Retrieving current state data from API...')
    r = requests.get(credentials.api1_url, auth=HTTPBasicAuth(credentials.api1_user, credentials.api1_pw))
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(r.json(), record_path='attributes', meta=['id', 'type'], sep='_')
    df['id'] = df.id.astype(int)
    return df


def retrieve_historical_data():
    headers = {'Authorization': f'Bearer {credentials.api2_token}'}
    # todo: request at most 1 week of data per call
    r = requests.get(credentials.api2_url, headers=headers)
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(r.json(), record_path='data')
    return df[['name', 'fromDate', 'toDate']]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
