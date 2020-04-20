import os

import requests
from requests.auth import AuthBase
from Crypto.Hash import HMAC
from Crypto.Hash import SHA256
from datetime import datetime
import json
import credentials
import pandas as pd
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


def call_fieldclimate_api(api_route, public_key, private_key, path, filename):
    auth = AuthHmacMetosGet(api_route, public_key, private_key)
    response = requests.get(apiURI + api_route, headers={'Accept': 'application/json'}, auth=auth)
    parsed = json.loads(response.text)
    # print(response.json())
    pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
    # print(pretty_resp)
    resp_file = open(os.path.join(path, filename), 'w+')
    # resp_file = open(r'/Users/jonasbieri/Documents/Meteoblue/python/data/json/' + filename + '.json', 'w+')
    resp_file.write(pretty_resp)

    normalized = pd.json_normalize(parsed)
    return (pretty_resp, normalized)


apiURI = 'https://api.fieldclimate.com/v1'

publicKey = credentials.publicKey
privateKey = credentials.privateKey

print('Retrieving information about all stations of current user from API...')
(pretty_resp, df) = call_fieldclimate_api('/user/stations', publicKey, privateKey, credentials.path, f'stations-{datetime.now()}')

print('Filtering Wolf stations...')
# mast_frame = stations_frame[stations_frame['name.custom'].str.contains('Mast') & ~stations_frame['name.custom'].str.contains('A2')]
wolf_df = df[df['name.custom'].str.contains('Wolf')]
filename_stations = f'/Users/jonasbieri/Documents/Meteoblue/python/data/csv/stations--{datetime.now()}.csv'
print(f'Saving Wolf stations to {filename_stations}...')
wolf_df.to_csv(filename_stations, index=False)

filename_stations_map = '/Users/jonasbieri/Documents/Meteoblue/python/data/csv/stations.csv'
print(f'Saving minimized table of station data for map creation to {filename_stations_map}')
wolf_map = wolf_df[['name.original', 'name.custom', 'dates.min_date',
                    'dates.max_date', 'position.altitude', 'position.geo.coordinates', 'config.timezone_offset']]


coords_rev = wolf_map['position.geo.coordinates'].apply(lambda x: [x[1], x[0]])
wolf_map['coords_rev'] = coords_rev
wolf_map.to_csv(filename_stations_map, index=False)

# print("Retrieving last hour's data from all Wolf stations from API...")
# for station in wolf_df['name.original']:
#     # get last data point from each station. See https://api.fieldclimate.com/v1/docs/#info-understanding-your-device
#     (pretty_resp, station_df) = call_fieldclimate_api('/data/normal/' + station + '/hourly/last/1h', publicKey, privateKey, credentials.path, f'station--{station}--{datetime.now()}')

print('Job successful!')
