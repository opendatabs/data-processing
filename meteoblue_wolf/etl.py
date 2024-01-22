from requests.auth import AuthBase
import logging
from Crypto.Hash import HMAC  # use package pycryptodome
from Crypto.Hash import SHA256
from datetime import datetime
import pathlib
import json
from meteoblue_wolf import credentials
import pandas as pd
import common


# Class to perform HMAC encoding
class AuthHmacMetosGet(AuthBase):
    # Creates HMAC authorization header for Metos REST service GET request.
    def __init__(self, api_route, public_key, private_key):
        self._publicKey = public_key
        self._privateKey = private_key
        self._method = 'GET'
        self._apiRoute = api_route

    def __call__(self, request):
        date_stamp = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        print("timestamp: ", date_stamp)
        request.headers['Date'] = date_stamp
        msg = (self._method + self._apiRoute + date_stamp + self._publicKey).encode(encoding='utf-8')
        h = HMAC.new(self._privateKey.encode(encoding='utf-8'), msg, SHA256)
        signature = h.hexdigest()
        request.headers['Authorization'] = 'hmac ' + self._publicKey + ':' + signature
        return request


def call_fieldclimate_api(api_uri, api_route, public_key, private_key, filename):
    auth = AuthHmacMetosGet(api_route, public_key, private_key)
    response = common.requests_get(url=api_uri + api_route, headers={'Accept': 'application/json'}, auth=auth)
    parsed = json.loads(response.text)
    # print(response.json())
    pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
    # print(pretty_resp)
    resp_file = open(f'{credentials.path}json/{filename}.json', 'w+')
    resp_file.write(pretty_resp)

    normalized = pd.json_normalize(parsed)
    return pretty_resp, normalized


def main():
    public_key = credentials.publicKey
    private_key = credentials.privateKey
    print('Retrieving information about all stations of current user from API...')
    (pretty_resp, df) = call_fieldclimate_api('https://api.fieldclimate.com/v2', '/user/stations', public_key,
                                              private_key, f'stations-{datetime.now()}')
    print('Filtering stations with altitude not set to null, only those are live...')
    # mast_frame = stations_frame[stations_frame['name.custom'].str.contains('Mast')
    #                             & ~stations_frame['name.custom'].str.contains('A2')]
    live_df = df.loc[pd.notnull(df['position.altitude'])]
    now = datetime.now()
    folder = now.strftime('%Y-%m')
    local_folder = f'{credentials.path}csv/val/{folder}'
    pathlib.Path(local_folder).mkdir(parents=True, exist_ok=True)
    filename_val = f"{local_folder}/stations--{now.strftime('%Y-%m-%dT%H-%M-%S%z')}.csv"
    logging.info(f'Ensuring columns exist...')
    column_names = ['name.original', 'name.custom', 'dates.min_date', 'dates.max_date', 'config.timezone_offset',
                    'meta.time', 'meta.rh', 'meta.airTemp', 'meta.rain24h.vals', 'meta.rain24h.sum', 'meta.rain48h.sum']
    for column_name in column_names:
        if column_name not in live_df.columns:
            live_df[column_name] = None
    print(f'Saving live stations to {filename_val}...')
    live_val = live_df[column_names]
    print("Getting last hour's precipitation...")
    pd.options.mode.chained_assignment = None  # Switch off warnings, see https://stackoverflow.com/a/53954986
    # make sure we have a list present, otherwise return None, see https://stackoverflow.com/a/12709152/5005585
    live_val['meta.rain.1h.val'] = live_df['meta.rain24h.vals'].apply(lambda x: x[23] if isinstance(x, list) else None)
    live_val.to_csv(filename_val, index=False)
    map_df = live_df[['name.original', 'name.custom', 'dates.min_date', 'dates.max_date', 'position.altitude',
                      'config.timezone_offset', 'position.geo.coordinates']]
    print('Stations with name.custom of length 1 are not live yet, filter those out...')
    # For some reason we have to filter > 2 here
    # map_df['name.custom.len'] = map_df['name.custom'].str.len()
    live_map = map_df.loc[map_df['name.custom'].str.len() > 2]
    # let's better do this in ODS, it gets nasty here for some reason.
    # print('Reversing coordinates for ods...')
    # live_map['coords'] = df['position.geo.coordinates'].apply(lambda x: [x[1], x[0]])
    filename_stations_map = f'{credentials.path}csv/map/stations.csv'
    print(f'Saving minimized table of station data for map creation to {filename_stations_map}')
    live_map.to_csv(filename_stations_map, index=False)
    # print("Retrieving last hour's data from all live stations from API...")
    # for station in df['name.original']:
    #     # get last data point from each station. See https://api.fieldclimate.com/v1/docs/#info-understanding-your-device
    #     (pretty_resp, station_df) = call_fieldclimate_api('/data/normal/' + station + '/hourly/last/1h',
    #                                                       publicKey, privateKey, f'station--{station}--{datetime.now()}')
    common.upload_ftp(filename_stations_map, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'map')
    common.ensure_ftp_dir(credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'val/{folder}')
    common.upload_ftp(filename_val, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, f'val/{folder}')
    print('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
