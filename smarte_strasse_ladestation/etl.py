import logging
from datetime import datetime, timedelta
import pandas as pd
import common
from smarte_strasse_ladestation import credentials


def main():
    latest_ods_start_time = get_latest_ods_start_time()
    from_filter = datetime.fromisoformat(latest_ods_start_time) - timedelta(days=7)
    logging.info(f'Latest starttime in ods: {latest_ods_start_time}, retrieving charges from {from_filter}...')

    token = authenticate()
    df = extract_data(token=token, from_filter=from_filter)
    size = df.shape[0]
    logging.info(f'{size} charges to be processed.')
    if size > 0:
        df_export = transform_data(df)
        load_data(df_export)
    logging.info(f'Job successful!')


def load_data(df_export):
    # export_file = os.path.join(credentials.data_path, f'charges_{date.today()}.csv')
    # df_export.to_csv(export_file, index=False)
    # common.upload_ftp(export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'smarte_strasse/elektroauto-ladestation/charges')
    #
    # {
    #     "startTime": "2022-01-17T20:08:21+02:00",
    #     "stopTime": "2022-01-18T08:42:20+02:00",
    #     "duration": 754,
    #     "wattHour": 37410,
    #     "connectorId": 2,
    #     "station.location.coordinates.lat": 47.54177,
    #     "station.location.coordinates.lng": 7.5880887,
    #     "kiloWattHour": 37.41,
    #     "station.capacity": 22,
    #     "station.connectorType": 2,
    #     "startTimeText": "2022-01-17T20:08:21+02:00",
    #     "stopTimeText": "2022-01-18T08:42:20+02:00",
    #     "station.location": "47.54177,7.5880887"
    # }

    df_export_json = df_export.to_json(orient="records")
    logging.info(f'Pushing {df_export.shape[0]} rows to ods realtime API...')
    r = common.requests_post(url=credentials.ods_push_api_url, data=df_export_json,
                             headers={'Authorization': f'apikey {credentials.api_key}'})

    r.raise_for_status()


def transform_data(df):
    logging.info(f'Transforming data for export...')
    df_export = df[['startTime', 'stopTime', 'duration', 'wattHour', 'connectorId', 'station.location.coordinates.lat', 'station.location.coordinates.lng']].copy(deep=True)
    df_export['kiloWattHour'] = df_export['wattHour'] / 1000
    df_export['station.capacity'] = 22
    df_export['station.connectorType'] = 2
    df_export['startTimeText'] = df_export.startTime
    df_export['stopTimeText'] = df_export.stopTime
    df_export['station.location'] = df_export['station.location.coordinates.lat'].astype(str) + ',' + df_export['station.location.coordinates.lng'].astype(str)
    return df_export


def get_latest_ods_start_time():
    logging.info(f'Getting latest entry from ODS dataset...')
    r = common.requests_get(url=credentials.ods_dataset_query_url)
    r.raise_for_status()
    record_count = len(r.json()['records'])
    # if dataset is empty: return 1970-01-01
    latest_ods_start_time = '1970-01-01T00:00:00+00:00' if record_count == 0 else r.json()['records'][0]['fields']['starttime']
    return latest_ods_start_time


def extract_data(token, from_filter):
    logging.info(f'Retrieving data...')
    headers = {'authorization': f'Bearer {token}', 'x-api-key': credentials.api_key}
    r = common.requests_get(url=f'{credentials.charges_url}', params={'perPage': 1000, 'from': from_filter}, headers=headers)
    r.raise_for_status()
    df = pd.json_normalize(r.json())
    return df


def authenticate():
    logging.info(f'Getting auth token...')
    payload = {'username': credentials.api_username, 'password': credentials.api_password}
    headers = {'x-api-key': credentials.api_key, 'content-type': 'application/json'}
    r = common.requests_post(url=credentials.auth_url, json=payload, headers=headers)
    return r.json()['token']


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
