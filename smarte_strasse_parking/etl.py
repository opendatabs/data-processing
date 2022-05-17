import datetime
import logging
import pandas as pd
from requests.auth import HTTPBasicAuth
import common
from smarte_strasse_parking import credentials


def main():
    df1 = get_current_state_data()
    # {"timestamp":"2022-02-03T16:43:09+00:00","Blue_occupied":4,"Yellow_occupied":2,"Blue_available":0,"Yellow_available":0,"Blue_total":4,"Yellow_total":2,"timestamp_text":"2022-02-03T16:43:09+00:00"}
    common.ods_realtime_push_df(df1, url=credentials.ods_push_url)
    # todo: Save csv file

    # common.ods_realtime_push_df(df1, url=credentials.ods_realtime_push_url_curr, push_key=credentials.ods_realtime_push_key_curr)
    # common.ods_realtime_push_df(df=df1, url=credentials.ods_realtime_push_url_curr, push_key=credentials.ods_realtime_push_key_curr)
    # push_timeseries_data(df=df1, min_time_delta_minutes=60, url=credentials.ods_realtime_push_url_hist, push_key=credentials.ods_realtime_push_key_hist, api_key=credentials.ods_api_key)

    # df = get_statistics()
    logging.info(f'Job successful!')


def get_statistics():
    headers = {'Authorization': f'Bearer {credentials.api3_token}'}
    dfs = []
    for spot in credentials.spots:
        spot_id = spot["id"]
        r = common.requests_get(f'{credentials.api3_stat_url}&spot={spot_id}', headers=headers)
        df = pd.json_normalize(r.json())
        df['spot_id'] = spot_id
        dfs.append(df)
    all_df = pd.concat(dfs)
    # todo:
    #  sum inflow and outflow per zone (yellow, blue) per hour
    #  avg occupancy per zone per hour

    return all_df


def get_current_state_data():
    logging.info(f'Retrieving current state data from API...')
    df_spots = pd.DataFrame.from_dict(credentials.spots)
    headers = {'Authorization': f'Bearer {credentials.api3_token}'}
    r = common.requests_get(credentials.api3_url, headers=headers)
    json = r.json()
    df = pd.json_normalize(json, record_path='spots', meta='latest_timestamp')
    df['id'] = df.id.astype(int)
    df_curr = df[['latest_timestamp', 'id', 'status']]
    df_merged = df_curr.merge(df_spots, on='id', how='left')
    df_min = df_merged[['latest_timestamp', 'status', 'type']].rename(columns={'latest_timestamp': 'timestamp'})
    df_count = df_min.groupby(['timestamp', 'status', 'type']).size().reset_index(name='count')
    df_pivot = pd.pivot_table(df_count, values=['count'], index=['timestamp'], columns=['type', 'status'], aggfunc='sum')
    df_wide = common.collapse_multilevel_column_names(df_pivot['count']).reset_index()
    # Ensure colums exist
    for column_name in ['Blue_occupied', 'Blue_available', 'Yellow_occupied',  'Yellow_available']:
        if column_name not in df_wide.columns:
            df_wide[column_name] = 0
    df_wide['Blue_total'] = 4
    df_wide['Yellow_total'] = 2
    df_wide['timestamp_text'] = df_wide.timestamp
    return df_wide


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()


    # 1 row in total
    # df = pd.json_normalize(json)
    # df_curr = df[['latest_timestamp', 'statistics.total.total_spots', 'statistics.total.occupied_spots', 'statistics.total.available_spots']]
    # df_curr = df_curr.rename(columns={
    #     'latest_timestamp': 'timestamp',
    #     'statistics.total.total_spots': 'total',
    #     'statistics.total.occupied_spots': 'occupied',
    #     'statistics.total.available_spots': 'available'
    # })
    # return df_curr


# def push_timeseries_data(df, min_time_delta_minutes, url, push_key, api_key):
#     logging.info(f'Checking timestamp of latest entry in time series...')
#     latest_data_url = f'https://data.bs.ch/api/records/1.0/search/?dataset=100171&q=&rows=1&sort=-value_updated&apikey={api_key}'
#     r = common.requests_get(url=latest_data_url)
#     json = r.json()
#     results = len(json['records'])
#     delta_minutes = -1
#     if results > 0:
#         latest_ods_record = datetime.datetime.fromisoformat(json['records'][0]['fields']['value_updated'])
#         current_df_time = datetime.datetime.strptime(df.value_updated.max(), '%Y-%m-%dT%H:%M:%S.%f%z')
#         delta_minutes = (current_df_time - latest_ods_record).seconds / 60
#     if results == 0 or delta_minutes >= min_time_delta_minutes:
#         logging.info(f'Pushing data time series dataset (minutes since last entry: {delta_minutes}).')
#         # Realtime API bootstrap data:
#         # {
#         #     "id": 0,
#         #     "value_updated": "2022-01-14T08:44:56.000Z",
#         #     "value_label": "1"
#         # }
#         common.ods_realtime_push_df(df, url, push_key)
#     else:
#         logging.info(f"It's not time yet to push into time series dataset (minutes since last entry: {delta_minutes}).")


# def push_detailed_historical_data():
#     logging.info(f'Retrieving historical data from API...')
#     todo1: use local time zone
#     now = datetime.datetime.now()
#     end = now.strftime('%Y%m%d%H%M')
#     start = (now - datetime.timedelta(days=7)).strftime('%Y%m%d%H%M')
#     headers = {'Authorization': f'Bearer {credentials.api2_token}'}
#     r = common.requests_get(credentials.api2_url, params={'timezone': 'Europe/Zurich', 'starts': start, 'ends': end}, headers=headers)
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
#     return df_hist



