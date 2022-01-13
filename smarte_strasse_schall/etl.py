import logging
import pandas as pd
from smarte_strasse_schall import credentials
import requests
from requests.auth import HTTPBasicAuth


def main():
    url = f'{credentials.url}api/devices/{credentials.device_id}'
    r = requests.get(url=url, auth=HTTPBasicAuth(credentials.username, credentials.password), timeout=credentials.request_timeout)
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(r.json())
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
