import requests
from requests.auth import AuthBase
from Crypto.Hash import HMAC
from Crypto.Hash import SHA256
from datetime import datetime
import json
import credentials


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


# Endpoint of the API, version for example: v1
apiURI = 'https://api.fieldclimate.com/v1'

# HMAC Authentication credentials
publicKey = credentials.publicKey
privateKey = credentials.privateKey

# Service/Route that you wish to call
apiRoute = '/user/stations'
apiRoute = '/data/0340015E'
apiRoute = '/data/0340015E/raw/last'

auth = AuthHmacMetosGet(apiRoute, publicKey, privateKey)
response = requests.get(apiURI+apiRoute, headers={'Accept': 'application/json'}, auth=auth)
parsed = json.loads(response.text)
# print(response.json())
pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
# print(pretty_resp)
resp_file = open(r'/data/response.json', 'w+')
resp_file.write(pretty_resp)
