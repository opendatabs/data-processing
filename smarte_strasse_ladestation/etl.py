import logging
import requests
from smarte_strasse_ladestation import credentials
import pandas as pd
import common


def main():
    token = authenticate()
    headers = {'authorization': f'Bearer {token}', 'x-api-key': credentials.api_key}
    # todo: define from url parameter so that we never get more than 1000 charges, see https://virta-partner-admin.api-docs.io/4.1/statistics-and-data-analysis
    r = common.requests_get(url=f'{credentials.charges_url}?from=2022-01-01T00%3A00%3A00%2B02%3A00', headers=headers)
    df = pd.json_normalize(r.json())
    pass


def authenticate():
    payload = {'username': credentials.api_username, 'password': credentials.api_password}
    headers = {'x-api-key': credentials.api_key, 'content-type': 'application/json'}
    r = common.requests_post(url=credentials.auth_url, json=payload, headers=headers)
    return r.json()['token']


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
