import os
import re
import logging
import pandas as pd
import datetime
from bs4 import BeautifulSoup

import common
from dotenv import load_dotenv

load_dotenv()


BASE_URL = "https://www.bs.ch"
LISTING_URL = "https://www.bs.ch/apps/regierungsratsbeschluesse"
DETAIL_PREFIX = "https://www.bs.ch/regierungsratsbeschluesse"

DATA_PATH = os.getenv("DATA_PATH")

def scrape_sitzung_overview(page_number, just_process_last_sitzung=False):
    """
    Returns a list of dicts:
      [
        { 'date': <string>,
          'detail_link': <string> }
        ...
      ]
    or an empty list if none found.
    """
    # Construct the paginated URL
    url = f"{LISTING_URL}?page={page_number}"
    logging.info(f"Scraping listing page: {url}")
    
    r = common.requests_get(url)
    r.raise_for_status()
    
    soup = BeautifulSoup(r.text, "html.parser")
    
    # Each “Sitzung” block could be found by a specific container.
    # From the sample HTML, each Sitzung seems to be in:
    # <div class="mb-25"> ... <button>DATE</button> ...
    # Then links <a href="/regierungsratsbeschluesse/..."></a> are inside <li class="text-base">.
    
    sitzung_blocks = soup.find_all("div", class_="mb-25")
    detail_urls = []
    
    for block in sitzung_blocks:
        button = block.find("button")
        if not button:
            continue
        
        date_text = button.get_text(strip=True)
        logging.info(f"   Found Sitzung: {date_text}")
        
        # Each anchor inside this block corresponds to a single “Geschäft”.
        links = block.find_all("a", href=re.compile(r"^/regierungsratsbeschluesse/"))
        for link in links:
            href = link["href"]
            # Typically looks like /regierungsratsbeschluesse/P245510?backUrl=/apps/...
            href_clean = href.split("?")[0]
            detail_url = BASE_URL + href_clean
            
            detail_urls.append(detail_url)
        if just_process_last_sitzung:
            break
    
    return detail_urls


def scrape_detail_page(url):
    """
    Scrape the detail page for a single Geschäft.
    Return a list of dicts, one for each Sitzung.
    Some fields may be empty if not found.
    """
    logging.info(f"   Scraping detail page: {url}")
    r = common.requests_get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    data = []
    # Initialize all fields to None
    praesidial_nr, titel, federfuehrung, parlamentarisch_text, parlamentarisch_url = None, None, None, None, None
    # Initialize all fields to None
    sitzung_datum, traktanden, regierungsratsbeschluss_url, weitere_dokumente = None, None, None, None

    tables = soup.find_all("table", class_="government-resolutions-data-table")
    # --- Geschäft: Präsidial-Nr., Titel, Federführung, Parlamentarisch ---
    geschaeft_table = tables[0] if tables else None
    if geschaeft_table:
        rows = geschaeft_table.find_all("tr")
        for row in rows:
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                logging.warning(f"   No th or td found for url {url} and tr {row}")
                continue
            
            label = th.get_text(strip=True)
            value = td.get_text(strip=True)

            if label == "Präsidial-Nr.":
                praesidial_nr = value
            elif label == "Titel":
                titel = value
            elif label == "Federführung":
                federfuehrung = value
            elif label == "Parlamentarisch":
                parlamentarisch_text = "Ja" if value.startswith("Ja") else value
                # Find the link to the PDF
                link = td.find("a")
                parlamentarisch_url = link["href"] if link else None
            else:
                logging.warning(f"   Unknown label: {label}")
    else:
        logging.warning(f"   No table found for url {url}")
    # --- Sitzung: Sitzung vom, Traktanden, Dokumente ---
    # One Geschäft can have multiple Sitzungen.
    if len(tables) > 1:
        for i in range(1, len(tables)):
            sitzung_table = tables[i]
            
            rows = sitzung_table.find_all("tr")
            for row in rows:
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    logging.warning(f"   No th or td found for url {url} and tr {row}")
                    continue

                label = th.get_text(strip=True)
                value = td.get_text(strip=True)

                if label == "Sitzung vom":
                    sitzung_datum = datetime.datetime.strptime(value, "%d.%m.%Y").strftime("%Y-%m-%d")
                # Traktanden
                elif label == "Traktanden":
                    traktanden = value
                # Dokumente
                elif label == "Dokumente":
                    # We want to find all <a> links
                    all_links = td.find_all("a")

                    regierungsratsbeschluss_url = None
                    weitere = []

                    for link in all_links:
                        link_text = link.get_text(strip=True)
                        link_href = link["href"]
                        # If the href is relative, build the full URL
                        if link_href.startswith("/"):
                            link_href = BASE_URL + link_href

                        # Check if the link text (or partial text) indicates “Regierungsratsbeschluss”
                        if "Regierungsratsbeschluss" in link_text:
                            regierungsratsbeschluss_url = link_href
                        else:
                            weitere.append(link_href)
                        weitere_dokumente = ",".join(weitere)
                else:
                    logging.warning(f"   Unknown label: {label}")

            data.append({
                'praesidial_nr': praesidial_nr,
                'titel': titel,
                'federfuehrung': federfuehrung,
                'parlamentarisch_text': parlamentarisch_text,
                'parlamentarisch_url': parlamentarisch_url,
                'sitzung_datum': sitzung_datum,
                'traktanden': traktanden,
                'regierungsratsbeschluss': regierungsratsbeschluss_url,
                'weitere_dokumente': weitere_dokumente,
                'url': url
            })
    else:
        data.append({
            'praesidial_nr': praesidial_nr,
            'titel': titel,
            'federfuehrung': federfuehrung,
            'parlamentarisch_text': parlamentarisch_text,
            'parlamentarisch_url': parlamentarisch_url,
            'sitzung_datum': sitzung_datum,
            'traktanden': traktanden,
            'regierungsratsbeschluss': regierungsratsbeschluss_url,
            'weitere_dokumente': weitere_dokumente,
            'url': url
        })
        logging.warning(f"   Less than 2 tables found for url {url}")

    return data


def main():
    just_process_last_sitzung = True

    all_data = []
    page_number = 1
    path_export = os.path.join(DATA_PATH, 'export', 'regierungsratsbeschluesse.csv')

    if just_process_last_sitzung:
        logging.info('Processing only the last sitzung...')
        sitzungen = scrape_sitzung_overview(1, just_process_last_sitzung)

        for sitzung in sitzungen:
            detail_data = scrape_detail_page(sitzung)
            all_data.append(detail_data)
        
        df = pd.read_csv(path_export)
        df_new_sitzung = pd.DataFrame(all_data)
        df = pd.concat([df, df_new_sitzung], ignore_index=True)
    else:
        logging.info('Processing all sitzungen...')
        while True:
            sitzungen = scrape_sitzung_overview(page_number)
            if not sitzungen:
                # No more sitzungen found on this page, so we stop.
                break
            
            for sitzung in sitzungen:
                detail_data = scrape_detail_page(sitzung)
                all_data += detail_data
            
            page_number += 1

        df = pd.DataFrame(all_data)
    # Drop duplicates since we are scraping multiple tables for each sitzung
    df = df.drop_duplicates()
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'staka/regierungsratsbeschluesse', '100427')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')