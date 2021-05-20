import requests
import os
import common
from bag_coronavirus import credentials


def file_path():
    return os.path.join(credentials.vmdl_path, credentials.vmdl_file)


def retrieve_vmdl_data():
    print(f'Retrieving vmdl data...')
    payload_token = f'client_id={credentials.vmdl_client_id}&scope={credentials.vmdl_scope}&username={credentials.vmdl_user}&password={credentials.vmdl_password}&grant_type=password'
    headers_token = {'Content-Type': 'application/x-www-form-urlencoded'}
    print(f'Getting OAUTH2 access token...')
    resp_token = requests.request("POST", credentials.vmdl_url_token, headers=headers_token, data=payload_token)
    resp_token.raise_for_status()
    # token_type = resp_token.json()['token_type']
    auth_string = f'Bearer {resp_token.json()["access_token"]}'

    payload_download={}
    headers_download = {'Authorization': auth_string}
    print(f'Downloading data...')
    resp_download = common.requests_get(credentials.vmdl_url_download, headers=headers_download, data=payload_download)
    resp_download.raise_for_status()
    file_path = os.path.join(credentials.vmdl_path, credentials.vmdl_file)
    print(f'Writing data to file {file_path}...')
    resp_download.encoding = 'utf-8'
    with open(file_path, "w") as f:
        f.write(resp_download.text)
    return file_path


def main():
    retrieve_vmdl_data()


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
