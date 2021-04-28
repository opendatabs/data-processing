import requests
import os
from bag_coronavirus import credentials

payload_token = f'client_id={credentials.vmdl_client_id}&scope={credentials.vmdl_scope}&username={credentials.vmdl_user}&password={credentials.vmdl_password}&grant_type=password'
headers_token = {'Content-Type': 'application/x-www-form-urlencoded'}
print(f'Getting OAUTH2 access token...')
resp_token = requests.request("POST", credentials.vmdl_url_token, headers=headers_token, data=payload_token)
resp_token.raise_for_status()
#token_type = resp_token.json()['token_type']
auth_string = f'Bearer {resp_token.json()["access_token"]}'

payload_download={}
headers_download = {'Authorization': auth_string}
print(f'Downloading data...')
resp_download = requests.request("GET", credentials.vmdl_url_download, headers=headers_download, data=payload_download)
resp_download.raise_for_status()
print(f'Writing data to file {credentials.vmdl_file}...')
with open(os.path.join(credentials.path, credentials.vmdl_file), "w") as f:
    f.write(resp_download.text)

print(f'Job successful!')