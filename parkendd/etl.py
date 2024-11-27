import os
import json
import logging
from datetime import datetime
from parkendd import credentials
import pandas as pd
import common
from common import change_tracking as ct
import ods_publish.etl_id as odsp
from bs4 import BeautifulSoup

def fetch_data_from_parkendd_api() -> pd.DataFrame:
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

    return normalized


def scrape_data_from_parkleitsystem() -> pd.DataFrame:
    url_to_scrape_from = "https://www.parkleitsystem-basel.ch/"
    logging.info(f'Scraping data from {url_to_scrape_from}...')
    
    response = common.requests_get(url_to_scrape_from)
    soup = BeautifulSoup(response.content, 'html.parser')

    parking_header = soup.find('h3', string='Freie Parkplätze').parent
    date_str = str(parking_header.find('p').contents[0]).strip()
    time_str = parking_header.find('span', class_='stempel_zeit').string.strip()

    timestamp = datetime.strptime(f"{date_str} {time_str}", '%d.%m.%Y %H:%M:%S')
    formatted_timestamp_last_updated = timestamp.strftime('%Y-%m-%dT%H:%M:%S')

    formatted_timestamp_now = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    lots_data = []
    for section in soup.find_all('section', class_='middle'):
        for table in section.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if not cells:
                    continue
                lot_data = {
                    'name': row.find('td', class_='parkh_name').get_text(strip=True),
                    'free': int(row.find('td', class_='parkh_belegung').get_text(strip=True)),
                    'status': row.find('td', class_='parkh_status').get_text(strip=True),
                    'last_updated': formatted_timestamp_last_updated,
                    'last_downloaded': formatted_timestamp_now
                }
                lots_data.append(lot_data)
    
    normalized_scraped = pd.DataFrame(lots_data)
    normalized_scraped['title'] = "Parkhaus " + normalized_scraped['name']
    normalized_scraped['id2'] = normalized_scraped['name'].str.lower().str.replace(' ', '')
    normalized_scraped['link'] = "https://www.parkleitsystem-basel.ch/parkhaus/" + normalized_scraped['id2']
    normalized_scraped['description'] = 'Anzahl freie Parkplätze: ' + normalized_scraped['free'].astype(str)
    normalized_scraped['published'] = normalized_scraped['last_downloaded']
    
    # Reorder columns to match the desired order
    column_order = ['address', 'forecast', 'free', 'id', 'lot_type', 'name', 'state', 'total', 
                    'last_downloaded', 'last_updated', 'coords.lat', 'coords.lng', 'title', 
                    'id2', 'link', 'description', 'published']
    normalized_scraped = normalized_scraped.reindex(columns=column_order)
    
    return normalized_scraped


def main():
    #normalized = fetch_data_from_parkendd_api()
    #print(normalized.head())
    normalized_scraped = scrape_data_from_parkleitsystem()
    exit()

    lots_file_name = os.path.join(credentials.path, 'csv', 'lots', 'parkendd-lots.csv')

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
