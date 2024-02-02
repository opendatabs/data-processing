import json
import logging
from datetime import datetime
from parkendd import credentials
import pandas as pd
import common
from common import change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    api_url = 'https://api.parkendd.de/Basel'
    logging.info(f'Getting latest data from {api_url}...')
    response = common.requests_get(url=api_url)

    logging.info(f'Parsing json...')
    parsed = json.loads(response.text)
    # pretty_resp = json.dumps(parsed, indent=4, sort_keys=True)
    # json_file_name = f'{credentials.path}json/parkendd-{str(datetime.now()).replace(":", "")}.json'
    # resp_file = open(json_file_name, 'w+')
    # resp_file.write(pretty_resp)
    # resp_file.close()

    lots_file_name = f'{credentials.path}csv/lots/parkendd-lots.csv'
    logging.info(f'Processing data...')
    for lot in parsed['lots']:
        lot['last_downloaded'] = parsed['last_downloaded']
        lot['last_updated'] = parsed['last_updated']

    normalized = pd.json_normalize(parsed, record_path='lots')
    normalized['title'] = "Parkhaus " + normalized['name']
    normalized['id2'] = normalized['id'].str.replace('baselparkhaus', '')
    normalized['link'] = "https://www.parkleitsystem-basel.ch/parkhaus/" + normalized['id2']
    normalized['description'] = 'Anzahl freie Parkplätze: ' + normalized['free'].astype(str)
    normalized['published'] = normalized['last_downloaded']

    logging.info(f'Creating lots file and saving as {lots_file_name}...')
    lots = normalized[
        ['address', 'id', 'lot_type', 'name', 'total', 'last_downloaded', 'last_updated', 'coords.lat', 'coords.lng',
         'title', 'id2', 'link', 'published']]
    lots.to_csv(lots_file_name, index=False)
    if ct.has_changed(lots_file_name):
        common.upload_ftp(lots_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          'parkendd/csv/lots')
        odsp.publish_ods_dataset_by_id('100044')
        ct.update_hash_file(lots_file_name)

    values_file_name = f'{credentials.path}csv/values/parkendd-{str(datetime.now()).replace(":", "")}.csv'
    logging.info(f'Creating values file and saving as {values_file_name}...')
    values = normalized[['published', 'free', 'id', 'id2']]
    values.to_csv(values_file_name, index=False)
    folder = datetime.now().strftime('%Y-%m')
    common.ensure_ftp_dir(credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'parkendd/csv/values/{folder}')
    common.upload_ftp(values_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                      f'parkendd/csv/values/{folder}')
    odsp.publish_ods_dataset_by_id('100014')

    logging.info('Job successful!')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
