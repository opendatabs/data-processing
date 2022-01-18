import logging
import datetime
import pandas as pd
import common
from smarte_strasse_schall import credentials
import requests
from requests.auth import HTTPBasicAuth


def main():
    url = f'{credentials.url}api/devices/{credentials.device_id}'
    auth = HTTPBasicAuth(credentials.username, credentials.password)
    r = requests.get(url=url, auth=auth, timeout=credentials.request_timeout)
    r.raise_for_status()
    json = r.json()
    df = pd.json_normalize(r.json())

    # todo: use local time zone
    now = datetime.datetime.now()
    end = now.isoformat()
    start = (now - datetime.timedelta(hours=6)).isoformat()
    r2 = common.requests_get(url=credentials.url + 'api/vehicle-detections', params={'start_time': start, 'size': '10000'}, auth=auth)
    r2.raise_for_status()
    json2 = r2.json()
    df2 = pd.json_normalize(r2.json(), record_path='results')

    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
