import datetime
import logging
import pandas as pd
from requests.auth import HTTPBasicAuth
import common
from smarte_strasse_parking import credentials


def main():
    df1 = get_current_state_date()
    common.ods_realtime_push_df(df=df1, url=credentials.ods_realtime_push_url_curr, push_key=credentials.ods_realtime_push_key_curr, api_key=credentials.ods_api_key)
    push_timeseries_data(df=df1, min_time_delta_minutes=60, url=credentials.ods_realtime_push_url_hist, push_key=credentials.ods_realtime_push_key_hist, api_key=credentials.ods_api_key)
    logging.info(f'Job successful!')


def get_current_state_date():
    logging.info(f'Retrieving current state data from API...')
    r = common.requests_get(credentials.api1_url, auth=HTTPBasicAuth(credentials.api1_user, credentials.api1_pw))
    r.raise_for_status()
    json = r.json()

    # showcase data
    df = pd.json_normalize(r.json(), record_path='attributes', meta=['id', 'type'], sep='_')
    df['id'] = df.id.astype(int)
    df_curr = df[['id', 'value_updated', 'value_label']]

    # 1 row per spot
    # headers = {'Authorization': f'Bearer {credentials.api3_token}'}
    # r = common.requests_get(credentials.api3_url, headers=headers)
    # r.raise_for_status()
    # json = r.json()
    # df = pd.json_normalize(json, record_path='spots', meta='latest_timestamp')
    # df['id'] = df.id.astype(int)
    # df_curr = df[['id', 'occupied', 'status']]

    # 1 row in total
    # df = pd.json_normalize(json)
    # df_curr = df[['latest_timestamp', 'statistics.total.total_spots', 'statistics.total.occupied_spots', 'statistics.total.available_spots']]

    return df_curr


def push_timeseries_data(df, min_time_delta_minutes, url, push_key, api_key):
    logging.info(f'Checking timestamp of latest entry in time series...')
    latest_data_url = f'https://data.bs.ch/api/records/1.0/search/?dataset=100171&q=&rows=1&sort=-value_updated&apikey={api_key}'
    r = common.requests_get(url=latest_data_url)
    r.raise_for_status()
    json = r.json()
    results = len(json['records'])
    delta_minutes = -1
    if results > 0:
        latest_ods_record = datetime.datetime.fromisoformat(json['records'][0]['fields']['value_updated'])
        current_df_time = datetime.datetime.strptime(df.value_updated.max(), '%Y-%m-%dT%H:%M:%S.%f%z')
        delta_minutes = (current_df_time - latest_ods_record).seconds / 60
    if results == 0 or delta_minutes >= min_time_delta_minutes:
        logging.info(f'Pushing data time series dataset (minutes since last entry: {delta_minutes}).')
        # Realtime API bootstrap data:
        # {
        #     "id": 0,
        #     "value_updated": "2022-01-14T08:44:56.000Z",
        #     "value_label": "1"
        # }
        common.ods_realtime_push_df(df, url, push_key, api_key)
    else:
        logging.info(f"It's not time yet to push into time series dataset (minutes since last entry: {delta_minutes}).")


# def push_detailed_historical_data():
#     logging.info(f'Retrieving historical data from API...')
#     todo1: use local time zone
#     now = datetime.datetime.now()
#     end = now.strftime('%Y%m%d%H%M')
#     start = (now - datetime.timedelta(days=7)).strftime('%Y%m%d%H%M')
#     headers = {'Authorization': f'Bearer {credentials.api2_token}'}
#     r = common.requests_get(credentials.api2_url, params={'timezone': 'Europe/Zurich', 'starts': start, 'ends': end}, headers=headers)
#     r.raise_for_status()
#     json = r.json()
#     df = pd.json_normalize(r.json(), record_path='data')
#     df['id'] = df.name.astype(int)
#     df_hist = df[['id', 'fromDate', 'toDate']]
#     row_count = len(df_hist)
#     if row_count == 0:
#         print(f'No rows to push to ODS... ')
#     else:
#         print(f'Pushing {row_count} rows to ODS realtime API...')
#         # Realtime API bootstrap data:
#         # {
#         #     "id": 0,
#         #     "fromDate": "2022-01-11T15:06:54+01:00",
#         #     "toDate": "2022-01-11T15:28:54+01:00"
#         # }
#         payload = df_hist.to_json(orient="records")
#         # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
#         # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
#         r = common.requests_post(url=credentials.ods_realtime_push_url_hist, data=payload, params={'pushkey': credentials.ods_realtime_push_key_curr, 'apikey': credentials.ods_api_key})
#         r.raise_for_status()
#     return df_hist


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
