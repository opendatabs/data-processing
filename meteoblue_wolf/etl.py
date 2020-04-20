import os
import requests
from requests.auth import AuthBase
from Crypto.Hash import HMAC  # use package pycryptodome
from Crypto.Hash import SHA256
from datetime import datetime
import json
from meteoblue_wolf import credentials
import pandas as pd
import common
import ast

# Class to perform HMAC encoding
class AuthHmacMetosGet(AuthBase):
    # Creates HMAC authorization header for Metos REST service GET request.
    def __init__(self, apiRoute, publicKey, privateKey):
        self._publicKey = publicKey
        self._privateKey = privateKey
        self._method = 'GET'
        self._apiRoute = apiRoute

    def __call__(self, request):
        dateStamp = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        print("timestamp: ", dateStamp)
        request.headers['Date'] = dateStamp
        msg = (self._method + self._apiRoute + dateStamp + self._publicKey).encode(encoding='utf-8')
        h = HMAC.new(self._privateKey.encode(encoding='utf-8'), msg, SHA256)
        signature = h.hexdigest()
        request.headers['Authorization'] = 'hmac ' + self._publicKey + ':' + signature
        return request


def call_fieldclimate_api(apiRoute, publicKey, privateKey, filename):
    auth = AuthHmacMetosGet(apiRoute, publicKey, privateKey)
    response = requests.get(apiURI + apiRoute, headers={'Accept': 'application/json'}, auth=auth)
    parsed = json.loads(response.text)
    # print(response.json())
    pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
    # print(pretty_resp)
    resp_file = open(f'{credentials.path}json/{filename}.json', 'w+')
    resp_file.write(pretty_resp)

    normalized = pd.json_normalize(parsed)
    return pretty_resp, normalized


apiURI = 'https://api.fieldclimate.com/v1'

publicKey = credentials.publicKey
privateKey = credentials.privateKey

print('Retrieving information about all stations of current user from API...')
(pretty_resp, df) = call_fieldclimate_api('/user/stations', publicKey, privateKey, f'stations-{datetime.now()}')

print('Filtering Wolf stations...')
# mast_frame = stations_frame[stations_frame['name.custom'].str.contains('Mast')
#                             & ~stations_frame['name.custom'].str.contains('A2')]
wolf_df = df[df['name.custom'].str.contains('Wolf')]
filename_val = f'{credentials.path}csv/val/stations--{datetime.now()}.csv'
print(f'Saving Wolf stations to {filename_val}...')
wolf_val = wolf_df[['name.original', 'name.custom', 'dates.min_date', 'dates.max_date', 'config.timezone_offset', 'meta.time', 'meta.rh', 'meta.airTemp', 'meta.rain24h.vals', 'meta.rain24h.sum', 'meta.rain48h.sum']]
print("Getting last hour's precipitation...")
pd.options.mode.chained_assignment = None  # Switch off warnings, see https://stackoverflow.com/a/53954986
wolf_val['meta.rain.1h.val'] = wolf_df['meta.rain24h.vals'].apply(lambda x: x[23])
wolf_val.to_csv(filename_val, index=False)

wolf_map = wolf_df[['name.original', 'name.custom', 'dates.min_date', 'dates.max_date', 'position.altitude', 'config.timezone_offset', 'position.geo.coordinates']]
print('Reversing coordinates for ods...')
wolf_map['coords'] = wolf_df['position.geo.coordinates'].apply(lambda x: [x[1], x[0]])
filename_stations_map = f'{credentials.path}csv/map/stations.csv'
print(f'Saving minimized table of station data for map creation to {filename_stations_map}')
wolf_map.to_csv(filename_stations_map, index=False)

# print("Retrieving last hour's data from all Wolf stations from API...")
# for station in wolf_df['name.original']:
#     # get last data point from each station. See https://api.fieldclimate.com/v1/docs/#info-understanding-your-device
#     (pretty_resp, station_df) = call_fieldclimate_api('/data/normal/' + station + '/hourly/last/1h',
#                                                       publicKey, privateKey, f'station--{station}--{datetime.now()}')


common.upload_ftp(filename_stations_map, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'map')
common.upload_ftp(filename_val, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'val')

print('Job successful!')
