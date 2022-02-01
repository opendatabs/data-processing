import logging
import datetime
from datetime import timezone
from zoneinfo import ZoneInfo
import numpy
import pandas as pd
import common
from smarte_strasse_schall import credentials
from requests.auth import HTTPBasicAuth


def main():
    auth = HTTPBasicAuth(credentials.username, credentials.password)
    df_vehicles = push_vehicles(auth)
    # df_sound_levels = push_sound_levels(auth)
    pass


def push_vehicles(auth):
    now = datetime.datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    # end = now.isoformat()
    start = (now - datetime.timedelta(hours=3)).isoformat()
    url = credentials.url + 'api/detections2'
    params = {'start_time': start, 'sort': 'timestamp', 'order': 'desc',  'size': '10000', 'filter': f'deviceId:{credentials.device_id}'}
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
    df_vehicles = df_class[['localDateTime', 'classificationIndex', 'classification']].copy(deep=True)
    # todo: Retrieve real values for speed and sound level as soon as API provides them
    df_vehicles['speed'] = numpy.NAN
    df_vehicles['level'] = numpy.NAN
    df_vehicles['timestamp_text'] = df_vehicles.localDateTime
    common.ods_realtime_push_df(df_vehicles, credentials.ods_dataset_url, credentials.ods_push_key)
    # {
    #     "localDateTime": "2022-01-19T08:17:13.896+01:00",
    #     "classificationIndex": -1,
    #     "classification": "Unknown",
    #     "timestamp_text": "2022-01-19T08:17:13.896+01:00",
    #     "speed": 49.9,
    #     "level": 50.1
    # }
    return df_vehicles


# todo: Implement as soon as API is ready.
def push_sound_levels(auth):
    now = datetime.datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    end = now.isoformat()
    start = (now - datetime.timedelta(hours=6)).isoformat()

    # r = common.requests_get(url=credentials.url + 'api/sound-levels', auth=auth, params={'start_time': start, 'size': '10000'})
    # r = common.requests_get(url=credentials.url + 'api/sound-levels/aggs/avg', auth=auth, params={'start_time': start, 'size': '10000'})

    # agg_type=avg does not seem to be used, despite being mentioned in the documentation
    # r = common.requests_get(url=credentials.url + 'api/sound-levels/unified?agg_type=avg&size=10000&field=level&start_time=2022-01-26T09:00:00.000Z&end_time=2022-01-26T09:15:00.000Z', auth=auth)  # ,  params={'start_time': start, 'size': '10000'})

    # with the following query I get the data we need, but only the raw values every 2.5 s, and only over a short time interval (not the interval defined in the url).
    # r = common.requests_get(url=credentials.url + 'api/sound-levels/unified?size=10000&field=level&start_time=2022-01-26T09:00:00.000Z&end_time=2022-01-26T09:05:00.000Z', auth=auth)  # ,  params={'start_time': start, 'size': '10000'})

    # GET /api/sound-levels/aggs/avg?timespan=5m&filter=deviceId:00000000XXXX&start_time=2022-02-01T14:00:00.000Z
    r = common.requests_get(url=credentials.url + f'api/sound-levels/aggs/avg?timespan=5m&filter=deviceId:{credentials.device_id}&start_time=2022-02-01T14:00:00.000Z', auth=auth)  # ,  params={'start_time': start, 'size': '10000'})
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(json['results'], record_path='levels', meta='timestamp')
    return df

    # manually querying all center_freq values seems to always retrieve the same value for each timestamp...!?
    # center_freqs = [25, 31.5, 40, 50, 63, 80, 100, 125, 160, 200, 250, 315, 400, 500, 630, 800, 1000, 1250, 1600, 2000, 2500, 3150, 4000, 5000, 6300, 8000, 10000, 12500, 16000]
    # dfs = []
    # for center_freq in center_freqs:
    #     r = common.requests_get(url=credentials.url + f'api/sound-levels/aggs/avg?field=level&timespan=15m&center_freq={center_freq}', auth=auth, params={'start_time': start, 'size': '10000'})
    #     r.raise_for_status()
    #     json = r.json()
    #     df = pd.json_normalize(json, record_path='results').assign(center_freq = center_freq)
    #     dfs.append(df)
    # # df_all = pd.concat([df.set_index('timestamp') for df in dfs], axis=1, join='outer').reset_index()
    # df_all = pd.concat(dfs, ignore_index=True)
    # r = common.requests_get(url=credentials.url + f'api/sound-levels/aggs/avg?field=level&timespan=15m', auth=auth, params={'start_time': start, 'size': '10000'})
    # r.raise_for_status()
    # json = r.json()
    # df = pd.json_normalize(json, record_path='results').assign(center_freq=0)
    # df_all = df_all.append(df)
    # df_pivot = df_all.pivot_table(columns=['center_freq'], values=['value'], index=['timestamp'])
    # return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
