from ods_harvest import credentials
import time
import common

import sys
ods_harvester_ids = sys.argv[1].split(',')


def wait_for_idle(harvester_id):
    while True:
        print(f'Checking status of harvester "{harvester_id}"...')
        resp = common.requests_get(url=f'https://data.bs.ch/api/automation/v1.0/harvesters/{harvester_id}/',
                                   headers={'Authorization': f'apikey {credentials.api_key}'})
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
        print(f'Problem with ODS command: HTTP Code {resp.status_code}')
        raise RuntimeError('Problem with OpenDataSoft Management API: ' + resp.text)


for harv_id in ods_harvester_ids:
    wait_for_idle(harv_id)
    print(f'Sending harvester "{harv_id}" the "start" signal...')
    response = common.requests_post(f'https://data.bs.ch/api/automation/v1.0/harvesters/{harv_id}/start/',
                                   headers={'Authorization': f'apikey {credentials.api_key}'})
    handle_http_errors(response)
    wait_for_idle(harv_id)

    print(f'Sending harvester "{harv_id}" the "publish" signal...')
    response = common.requests_post(f'https://data.bs.ch/api/automation/v1.0/harvesters/{harv_id}/publish/',
                                   headers={'Authorization': f'apikey {credentials.api_key}'})
    handle_http_errors(response)
    wait_for_idle(harv_id)


print('Job successful!')
