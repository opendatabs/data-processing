from ods_harvest import credentials
import time
import requests

import sys
ods_harvester_ids = sys.argv[1].split(',')


def wait_for_idle(harvester_id):
    while True:
        print(f'Checking status of harvester "{harvester_id}"...')
        resp = requests.get(f'https://basel-stadt.opendatasoft.com/api/management/v2/harvesters/{harvester_id}/', auth=(credentials.ods_user, credentials.ods_password), proxies={'https': credentials.proxy})
        handle_http_errors(resp)
        status = resp.json()['status']
        print(f'Harvester "{harvester_id}" is "{status}".')
        if status == 'idle':
            break
        else:
            seconds = 10
            print(f'Waiting {seconds} seconds before trying again...')
            time.sleep(seconds)


def handle_http_errors(resp):
    if resp.status_code == 200:
        print(f'ODS command successful: HTTP Code {resp.status_code}')
    else:
        print(f'Problem with ODS command: HTTP Code "{resp.status_code}"')
        raise RuntimeError('Problem with OpenDataSoft Management API: ' + resp.text)


for harv_id in ods_harvester_ids:
    wait_for_idle(harv_id)
    print(f'Sending harvester "{harv_id}" the "start" signal...')
    response = requests.put(f'https://basel-stadt.opendatasoft.com/api/management/v2/harvesters/{harv_id}/start/', auth=(credentials.ods_user, credentials.ods_password), proxies={'https': credentials.proxy})
    handle_http_errors(response)
    wait_for_idle(harv_id)

    print(f'Sending harvester "{harv_id}" the "publish" signal...')
    response = requests.put(f'https://basel-stadt.opendatasoft.com/api/management/v2/harvesters/{harv_id}/publish/', auth=(credentials.ods_user, credentials.ods_password), proxies={'https': credentials.proxy})
    handle_http_errors(response)
    wait_for_idle(harv_id)


print('Job successful!')