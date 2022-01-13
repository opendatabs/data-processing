import logging

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import common
from smarte_strasse_parking import credentials


def main():
    r = requests.get(credentials.api_url, auth=HTTPBasicAuth(credentials.api_user, credentials.api_pw))
    r.raise_for_status()
    json = r.json()
    df1 = pd.json_normalize(r.json(), record_path='attributes', meta=['id', 'type'])
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
