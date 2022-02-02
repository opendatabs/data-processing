import logging
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
import numpy
import numpy as np
import pandas as pd
import common
from smarte_strasse_schall import credentials
from requests.auth import HTTPBasicAuth


def main():
    auth = HTTPBasicAuth(credentials.username, credentials.password)
    df_vehicles = push_vehicles(auth)
    df_sound_levels = push_noise_levels(auth)
    df_vehicles_speed = push_vehicle_speed_level(auth)
    logging.info(f'Job succcessful!')
    pass


def push_vehicles(auth):
    logging.info(f'Starting process to update vehicles dataset...')
    now = datetime.datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    # end = now.isoformat()
    start = (now - datetime.timedelta(hours=3)).isoformat()
    url = credentials.url + 'api/detections2'
    params = {'start_time': start, 'sort': 'timestamp', 'order': 'desc',  'size': '10000', 'filter': f'deviceId:{credentials.device_id}'}
    df_class = query_sensor_api(auth, params, url)
    df_vehicles = df_class[['localDateTime', 'classificationIndex', 'classification']].copy(deep=True)
    # todo: Retrieve real values for speed and sound level as soon as API provides them
    df_vehicles['speed'] = numpy.NAN
    df_vehicles['level'] = numpy.NAN
    df_vehicles['timestamp_text'] = df_vehicles.localDateTime
    common.ods_realtime_push_df(df_vehicles, credentials.ods_dataset_url_veh, credentials.ods_push_key_veh)
    # {
    #     "localDateTime": "2022-01-19T08:17:13.896+01:00",
    #     "classificationIndex": -1,
    #     "classification": "Unknown",
    #     "timestamp_text": "2022-01-19T08:17:13.896+01:00",
    #     "speed": 49.9,
    #     "level": 50.1
    # }
    logging.info(f'That worked out successfully!')
    return df_vehicles


def push_vehicle_speed_level(auth):
    logging.info(f'Starting process to update vehicles with speed and noise level dataset...')
    now = datetime.datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    # If the dataset is empty, set a default start timestampt for the api call
    # start = (now - datetime.timedelta(hours=37)).isoformat()
    start = '2022-02-01T00:00:00.000000+01:00'
    end = now.isoformat()
    logging.info(f'Checking timestamp of latest entry ods...')
    latest_data_url = f'https://data.bs.ch/api/records/1.0/search/?dataset=100175&q=&rows=1&sort=localdatetime_interval_end&apikey={credentials.ods_api_key}'
    r = common.requests_get(url=latest_data_url)
    json = r.json()
    results = len(json['records'])
    if results > 0:
        # start = (datetime.datetime.fromisoformat(json['records'][0]['fields']['localdatetime_interval_end_text']) - datetime.timedelta(milliseconds=1)).isoformat()
        start = datetime.datetime.fromisoformat(json['records'][0]['fields']['localdatetime_interval_end_text']).isoformat()

    url = credentials.url + 'api/detections2'
    params = {'start_time': start, 'end_time': end, 'sort': 'timestamp', 'order': 'desc',  'size': '10000', 'filter': f'deviceId:{credentials.device_id}'}
    df_class = query_sensor_api(auth, params, url)
    df_reorder = df_class.sort_values(by='localDateTime').reset_index(drop=True)
    # select every 5th row,starting once from 0 and once from 1. See also https://stackoverflow.com/a/25057724
    n = 5
    df5 = df_reorder.iloc[::n, :]
    df_merged = df_reorder.merge(right=df5, how='left', left_index=True, right_index=True, suffixes=(None, '_start'))
    logging.info(f'Calculating start and end timestamp of each interval of {n} vehicles...')
    df_merged['localDateTime_interval_end'] = df_merged.localDateTime_start.shift(-1)
    df_merged['localDateTime_interval_start'] = df_merged.localDateTime_start.fillna(method='ffill')
    df_merged['localDateTime_interval_end'] = df_merged.localDateTime_interval_end.fillna(method='bfill')
    logging.info(f"Removing last interval in which we don't have {n} vehicles yet..")
    df_merged = df_merged.dropna(subset=['localDateTime_interval_end'])
    logging.info(f'Calculating interval length...')
    df_merged['interval_length_seconds'] = (pd.to_datetime(df_merged.localDateTime_interval_end) - pd.to_datetime(df_merged.localDateTime_interval_start)).dt.total_seconds()
    df_test = df_merged[['localDateTime', 'localDateTime_start', 'localDateTime_interval_start', 'localDateTime_interval_end', 'level', 'speed', 'interval_length_seconds']].copy(deep=True)
    df_vehicles = df_merged[['localDateTime_interval_start', 'localDateTime_interval_end', 'level', 'speed', 'interval_length_seconds']].copy(deep=True)

    df_vehicles['localDateTime_interval_start_text'] = df_vehicles.localDateTime_interval_start
    df_vehicles['localDateTime_interval_end_text'] = df_vehicles.localDateTime_interval_end
    # Random sorting within time interval
    df_vehicles['random_number'] = np.random.randint(0, 10000, size=(len(df_vehicles), 1))
    df_vehicles = (df_vehicles
                   .sort_values(by=['localDateTime_interval_start', 'random_number'])
                   .reset_index(drop=True)
                   .drop(columns=['random_number'])
                   )
    df_vehicles['vehicle_rand_number'] = df_vehicles.index % 5
    common.ods_realtime_push_df(df_vehicles, credentials.ods_dataset_url_veh_speed, credentials.ods_push_key_veh_speed)
    logging.info(f'That worked out successfully!')
    return df_vehicles

    # {
    #     "vehicle_rand_number": 0,
    #     "localDateTime_interval_start": "2022-02-02T08:40:13.875+01:00",
    #     "localDateTime_interval_end": "2022-02-02T08:50:45.923+01:00",
    #     "level": 30.0,
    #     "speed": 20.4,
    #     "interval_length_seconds": 121.735,
    #     "localDateTime_interval_start_text": "2022-02-02T08:44:13.875+01:00",
    #     "localDateTime_interval_end_text": "2022-02-02T08:44:45.923+01:00"
    # }


def query_sensor_api(auth, params, url):
    logging.info(f'Querying API using url {url} with parameters {params}...')
    r = common.requests_get(url=url, params=params, auth=auth)
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(json, record_path='results')
    df_classifications = pd.DataFrame.from_dict({
        'classificationIndex': [-1, 0, 1, 2, 3],
        'classification': ['Unknown', 'Car', 'Bicycle / Motorbike', 'Truck / Bus', 'Van / Suv']
    })
    df_class = df.merge(df_classifications, on='classificationIndex', how='left')
    logging.info(f'Received {len(df_class)} records from API...')
    return df_class


def push_noise_levels(auth):
    logging.info(f'Starting process to update noise level dataset...')
    now = datetime.datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    # end = now.isoformat()
    # datetime needed in military "Zulu" notation using %Z
    start = (now - datetime.timedelta(hours=3)).astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    r = common.requests_get(url=credentials.url + f'api/sound-levels/aggs/avg', params={'timespan': '5m', 'filter': f'deviceId:{credentials.device_id}', 'start_time': start}, auth=auth)
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(json['results'], record_path='levels', meta=['general_level', 'timestamp'])
    # make sure we get the correct column order by converting center_freq to str with zero-padding
    df.center_freq = df.center_freq.str.zfill(7).replace('\.0', '', regex=True)
    df_pivot = df.pivot_table(columns=['center_freq'], values=['level'], index=['timestamp', 'general_level']).reset_index()
    df_pivot = common.collapse_multilevel_column_names(df_pivot)
    df_pivot = df_pivot.rename(columns={'timestamp_': 'timestamp', 'general_level_': 'general_level'})
    df_pivot['timestamp_text'] = df_pivot.timestamp
    common.ods_realtime_push_df(df_pivot, credentials.ods_dataset_url_noise, credentials.ods_push_key_noise)
    logging.info(f'That worked out successfully!')
    return df_pivot

# {
#     "timestamp":"2022-02-02T05:00:00.000Z",
#     "general_level":45.6390674286,
#     "level_00025":75.4284092296,
#     "level_00031.5":70.0897727446,
#     "level_00040":64.7340911952,
#     "level_00050":62.1863636104,
#     "level_00063":57.2306817662,
#     "level_00080":49.3363636624,
#     "level_00100":46.9568182338,
#     "level_00125":47.5886362683,
#     "level_00160":48.9204545021,
#     "level_00200":53.775000052,
#     "level_00250":40.3454545628,
#     "level_00315":40.962499922,
#     "level_00400":40.2965911085,
#     "level_00500":44.3704547449,
#     "level_00630":45.4193181558,
#     "level_00800":44.9034093077,
#     "level_01000":44.8590909568,
#     "level_01250":44.8306817358,
#     "level_01600":44.5568181168,
#     "level_02000":40.801136342,
#     "level_02500":39.0386363593,
#     "level_03150":38.1295454936,
#     "level_04000":33.3102273291,
#     "level_05000":36.4613636407,
#     "level_06300":33.539772814,
#     "level_08000":29.9056818052,
#     "level_10000":36.0590908744,
#     "level_12500":37.0579545281,
#     "level_16000":32.438636368,
#     "timestamp_text":"2022-02-02T05:00:00.000Z"
# }


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
