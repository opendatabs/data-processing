import os
import re
import logging
import requests
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
    results = []
    
    for block in sitzung_blocks:
        button = block.find("button")
        if not button:
            continue
        
        date_text = button.get_text(strip=True)
        # Convert the date format
        date_text = datetime.datetime.strptime(date_text, "%d.%m.%Y").strftime("%Y-%m-%d")
        logging.info(f"   Found Sitzung: {date_text}")
        
        # Each anchor inside this block corresponds to a single “Geschäft”.
        links = block.find_all("a", href=re.compile(r"^/regierungsratsbeschluesse/"))
        for link in links:
            href = link["href"]
            # Typically looks like /regierungsratsbeschluesse/P245510?backUrl=/apps/...
            href_clean = href.split("?")[0]
            detail_url = BASE_URL + href_clean
            
            results.append({
                "date": date_text,
                "detail_link": detail_url
            })
        if just_process_last_sitzung:
            break
    
    return results


def scrape_detail_page(url, expected_date):
    """
    Scrape the detail page for a single Geschäft.
    Return a dict with:
      {
         'praesidial_nr': str,
         'titel': str,
         'federfuehrung': str,
         'parlamentarisch_text': str,
         'parlamentarisch_url': str,
         'sitzung_datum': str,
         'traktanden': str,
         'regierungsratsbeschluss': str,
         'weitere_dokumente': str
      }
    Some fields may be empty if not found.
    """
    logging.info(f"   Scraping detail page: {url}")
    r = common.requests_get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    
    data = {
        'praesidial_nr': None,
        'titel': None,
        'federfuehrung': None,
        'parlamentarisch_text': None,
        'parlamentarisch_url': None,
        'sitzung_datum': expected_date,
        'traktanden': None,
        'regierungsratsbeschluss': None,
        'weitere_dokumente': None,
        'url': url
    }
    
    # --- First table: Präsidial-Nr., Titel, Federführung, Parlamentarisch ---
    geschaeft_table = soup.find("table", class_="government-resolutions-data-table")
    if geschaeft_table:
        rows = geschaeft_table.find_all("tr")
        for row in rows:
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue
            
            label = th.get_text(strip=True)
            value = td.get_text(strip=True)
            
            if label == "Präsidial-Nr.":
                data['praesidial_nr'] = value
            elif label == "Titel":
                data['titel'] = value
            elif label == "Federführung":
                data['federfuehrung'] = value
            elif label == "Parlamentarisch":
                # We expect the text to be something like "Ja" or "Nein"
                # plus a link to the GRIBS site if it exists
                # e.g. "Ja" <a href="...">[Grossratsinformationssystem - GRIBS]</a>
                data['parlamentarisch_text'] = value.replace("[Grossratsinformationssystem - GRIBS]", "").strip()
                a = td.find("a")
                if a:
                    data['parlamentarisch_url'] = a["href"]
    
    # --- Second table: Sitzung vom, Traktanden, Dokumente ---
    # We want the row that matches the `expected_date` for "Sitzung vom"
    tables = soup.find_all("table", class_="government-resolutions-data-table")
    if len(tables) > 1:
        for i in range(1, len(tables)):
            sitzungen_table = tables[i]
            
            rows = sitzungen_table.find_all("tr")
            chunk_size = 3
            for j in range(0, len(rows), chunk_size):
                chunk = rows[j:j+chunk_size]
                if len(chunk) < 3:
                    break
                
                # Sitzung vom: Verify the date
                sitzung_label = chunk[0].find("th").get_text(strip=True)
                sitzung_date = chunk[0].find("td").get_text(strip=True)

                # Convert the date format
                sitzung_date = datetime.datetime.strptime(sitzung_date, "%d.%m.%Y").strftime("%Y-%m-%d")
                
                if sitzung_label == "Sitzung vom" and sitzung_date == expected_date:
                    # Traktanden
                    traktanden_label = chunk[1].find("th").get_text(strip=True)
                    traktanden_value = chunk[1].find("td").get_text(strip=True)
                    if traktanden_label == "Traktanden":
                        data['traktanden'] = traktanden_value
                    
                    # Dokumente
                    dokumente_label = chunk[2].find("th").get_text(strip=True)
                    dokumente_td = chunk[2].find("td")
                    if dokumente_label == "Dokumente" and dokumente_td:
                        # We want to find all <a> links
                        all_links = dokumente_td.find_all("a")
                        
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
                        
                        data['regierungsratsbeschluss'] = regierungsratsbeschluss_url
                        data['weitere_dokumente'] = ", ".join(weitere) if weitere else None
                    
                    # We got what we needed; break out of the loop
                    break

    return data


def main():
    just_process_last_sitzung = False

    all_data = []
    page_number = 1
    path_export = os.path.join(DATA_PATH, 'export', 'regierungsratsbeschluesse.csv')

    if just_process_last_sitzung:
        logging.info('Processing only the last sitzung...')
        sitzungen = scrape_sitzung_overview(1, just_process_last_sitzung)

        for s in sitzungen:
            detail_data = scrape_detail_page(s["detail_link"], s["date"])
            all_data.append(detail_data)
        
        df = pd.read_csv(path_export)
        df_new_sitzung = pd.DataFrame(all_data)
        df = pd.concat([df, df_new_sitzung], ignore_index=True)
        df = df.drop_duplicates(subset=['praesidial_nr', 'sitzung_datum'], keep='last')
    else:
        logging.info('Processing all sitzungen...')
        while True:
            sitzungen = scrape_sitzung_overview(page_number)
            if not sitzungen:
                # No more sitzungen found on this page, so we stop.
                break
            
            for s in sitzungen:
                detail_data = scrape_detail_page(s["detail_link"], s["date"])
                all_data.append(detail_data)
            
            page_number += 1
    
        df = pd.DataFrame(all_data)
    
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'staka/regierungsratsbeschluesse', '100427')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')