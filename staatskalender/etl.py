import logging
from requests.auth import HTTPBasicAuth

import common
from staatskalender import credentials


def main():
    res_auth = common.requests_get('https://staatskalender.bs.ch/api/authenticate',
                                auth=HTTPBasicAuth(credentials.access_key, ''))
    res_auth.raise_for_status()
    token = res_auth.json()['token']
    r = common.requests_get('https://staatskalender.bs.ch/api/agencies?title=datenschutzbeauftragter',
                            auth=HTTPBasicAuth(token, ''))
    r.raise_for_status()
    print(r.json())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")