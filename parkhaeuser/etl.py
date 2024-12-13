import logging
import pandas as pd
import common
from datetime import datetime
from bs4 import BeautifulSoup
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    push_key = os.getenv('PUSHKEY')

    # Scrape website
    url_to_scrape_from = "https://www.parkleitsystem-basel.ch/"
    logging.info(f'Scraping data from {url_to_scrape_from}...')

    response = common.requests_get(url_to_scrape_from)
    soup = BeautifulSoup(response.content, 'html.parser')

    parking_header = soup.find('h3', string='Freie Parkplätze').parent
    date_str = str(parking_header.find('p').contents[0]).strip()
    time_str = parking_header.find('span', class_='stempel_zeit').string.strip()
    local_timezone = ZoneInfo("Europe/Zurich")
    timestamp = datetime.strptime(f"{date_str} {time_str}", '%d.%m.%Y %H:%M:%S').replace(tzinfo=local_timezone)
    published = timestamp.isoformat(timespec='seconds')
    formatted_timestamp_now = datetime.now(local_timezone).isoformat(timespec='seconds')

    #print(f"Last updated at    {published}")
    #print(f"Last downloaded at {formatted_timestamp_now}")
    #print()

    lots_data = []
    for section in soup.find_all('section', class_='middle'):
        for table in section.find_all('table'):
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if not cells:
                    continue

                link_element = row.find('td', class_='parkh_name').find('a')
                href = link_element['href']

                if href.count('/') != 1:
                    raise ValueError(f"Invalid href format: {href}. Expected exactly one '/'")

                prefix, id2 = href.split('/')
                link = url_to_scrape_from + href
                free = int(row.find('td', class_='parkh_belegung').get_text(strip=True))
                name = row.find('td', class_='parkh_name').get_text(strip=True)
                status = row.find('td', class_='parkh_status').get_text(strip=True)
                title = f"{prefix.capitalize()} {name}"

                lot_data = {
                    'free': free,
                    'href': href,
                    'id': f'basel{prefix}{id2}',
                    'id2': id2,
                    'last_downloaded': formatted_timestamp_now,
                    'published': published,
                    'link': link,
                    'lot_type': prefix.capitalize(),
                    'name': name,
                    'status': status,
                    'title': title
                }

                lots_data.append(lot_data)

    df_scraped = pd.DataFrame(lots_data)

    df_for_upload = df_scraped[['id', 'title', 'free', 'status', 'published', 'last_downloaded', 'link']].copy()

    url_rtp = "https://data.bs.ch/api/push/1.0/100088/echtzeit/push/"
    common.ods_realtime_push_df(df_for_upload,
                                url= url_rtp,
                                push_key=push_key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
