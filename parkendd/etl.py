import logging
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import common
import pandas as pd
from bs4 import BeautifulSoup
from common import FTP_PASS, FTP_SERVER, FTP_USER


def find_additional_info(url_lot: str):
    additional_info = {}

    # Take address and coords from parkhaeuser_manually_curated.csv
    # Scrape total and durchfahrtshoehe from website

    # Get webpage content
    response = common.requests_get(url=url_lot)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    try:
        total_str = soup.find_all("b", string="Kurzparkplätze:")[0].next_sibling.strip()
    except IndexError:
        total_str = None

    if total_str is not None:
        try:
            total = int(total_str)
        except ValueError:
            regex_badbahnhof = r"(\d+) \(Mo.Fr\), (\d+) \(Sa.So\)"
            regex_results_badbahnhof = re.findall(regex_badbahnhof, total_str)

            regex_messe = r"(\d+) \+ (\d+) IV-Parkplätze im 4\. OG"
            regex_results_messe = re.findall(regex_messe, total_str)

            regex_claramatte = r"Mo\..Fr\. (\d+) \/ Sa\..So\. (\d+)"
            regex_results_claramatte = re.findall(regex_claramatte, total_str)

            if len(regex_results_badbahnhof) == 1:
                regex_results_badbahnhof_ints = [
                    int(item) for item in regex_results_badbahnhof[0]
                ]
                total = max(regex_results_badbahnhof_ints)
            elif len(regex_results_messe) == 1:
                regex_results_messe_ints = [
                    int(item) for item in regex_results_messe[0]
                ]
                total = sum(regex_results_messe_ints)
            elif len(regex_results_claramatte) == 1:
                regex_results_claramatte_ints = [
                    int(item) for item in regex_results_claramatte[0]
                ]
                total = max(regex_results_claramatte_ints)
            else:
                exit(
                    f"Error: Number of total is present on {url_lot} but cannot be extracted. The total is described as '{total_str}'. Maybe the regex is outdated?"
                )
    else:
        total = None
        logging.info(f"No total found for {url_lot}")

    additional_info["total"] = total

    try:
        durchfahrtshoehe_str = soup.find_all("b", string="Durchfahrtshöhe:")[
            0
        ].next_sibling.strip()
    except IndexError:
        durchfahrtshoehe_str = ""

    additional_info["durchfahrtshoehe"] = durchfahrtshoehe_str

    # Read CSV and get additional info
    csv_path_of_manually_curated = os.path.join(
        "data", "csv", "lots", "parkhaeuser_manually_curated.csv"
    )
    df = pd.read_csv(csv_path_of_manually_curated)
    lot_info = df.set_index("link").loc[url_lot]

    additional_info["total"] = total
    additional_info["address"] = lot_info["address"]
    additional_info["coords.lat"] = lot_info["coords.lat"]
    additional_info["coords.lng"] = lot_info["coords.lng"]

    return additional_info


def scrape_data_from_parkleitsystem() -> pd.DataFrame:
    url_to_scrape_from = "https://www.parkleitsystem-basel.ch/"
    logging.info(f"Scraping data from {url_to_scrape_from}...")

    response = common.requests_get(url_to_scrape_from)
    soup = BeautifulSoup(response.content, "html.parser")

    parking_header = soup.find("h3", string="Freie Parkplätze").parent
    date_str = str(parking_header.find("p").contents[0]).strip()
    time_str = parking_header.find("span", class_="stempel_zeit").string.strip()

    formatted_timestamp_last_updated = (
        datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M:%S")
        .replace(tzinfo=ZoneInfo("Europe/Zurich"))
        .isoformat(timespec="seconds")
    )

    formatted_timestamp_now = datetime.now(ZoneInfo("Europe/Zurich")).isoformat(
        timespec="seconds"
    )

    lots_data = []
    for section in soup.find_all("section", class_="middle"):
        for table in section.find_all("table"):
            for row in table.find_all("tr"):
                cells = row.find_all("td")
                if not cells:
                    continue

                link_element = row.find("td", class_="parkh_name").find("a")
                href = link_element["href"]

                if href.count("/") != 1:
                    raise ValueError(
                        f"Invalid href format: {href}. Expected exactly one '/'"
                    )

                prefix, id2 = href.split("/")

                status = row.find("td", class_="parkh_status").get_text(strip=True)
                state = (
                    "open"
                    if status == "offen"
                    else "closed"
                    if status == "zu"
                    else "unknown state"
                )

                url_lot = url_to_scrape_from + href
                additional_info_scraped = find_additional_info(url_lot=url_lot)

                lot_data = {
                    "name": row.find("td", class_="parkh_name").get_text(strip=True),
                    "free": int(
                        row.find("td", class_="parkh_belegung").get_text(strip=True)
                    ),
                    "status": status,
                    "state": state,
                    "last_updated": formatted_timestamp_last_updated,
                    "last_downloaded": formatted_timestamp_now,
                    "href": href,
                    "id": f"basel{prefix}{id2}",
                    "id2": id2,
                    "lot_type": prefix.capitalize(),
                    "total": additional_info_scraped["total"],
                    "durchfahrtshoehe": additional_info_scraped["durchfahrtshoehe"],
                    "address": additional_info_scraped["address"],
                    "coords.lat": additional_info_scraped["coords.lat"],
                    "coords.lng": additional_info_scraped["coords.lng"],
                }
                lots_data.append(lot_data)

    normalized_scraped = pd.DataFrame(lots_data)
    normalized_scraped["title"] = (
        normalized_scraped["lot_type"] + " " + normalized_scraped["name"]
    )
    normalized_scraped["link"] = url_to_scrape_from + normalized_scraped["href"]
    normalized_scraped["published"] = normalized_scraped["last_downloaded"]

    return normalized_scraped


def main():
    lots_file_name = os.path.join("data", "csv", "lots", "parkendd-lots.csv")

    normalized = scrape_data_from_parkleitsystem()

    logging.info(f"Creating lots file and saving as {lots_file_name}...")
    lots = normalized[
        [
            "address",
            "id",
            "lot_type",
            "name",
            "total",
            "last_updated",
            "coords.lat",
            "coords.lng",
            "title",
            "id2",
            "state",
            "durchfahrtshoehe",
            "lot_type",
            "link",
            "published",
        ]
    ]
    lots.to_csv(lots_file_name, index=False)
    common.update_ftp_and_odsp(lots_file_name, "parkendd/csv/lots", "100044")

    values_file_name = (
        f"data/csv/values/parkendd-{str(datetime.now()).replace(':', '')}.csv"
    )
    logging.info(f"Creating values file and saving as {values_file_name}...")
    values = normalized[["published", "free", "id", "id2"]]
    values.to_csv(values_file_name, index=False)
    folder = datetime.now().strftime("%Y-%m")
    common.ensure_ftp_dir(
        FTP_SERVER, FTP_USER, FTP_PASS, f"parkendd/csv/values/{folder}"
    )
    common.update_ftp_and_odsp(
        values_file_name, f"parkendd/csv/values/{folder}", "100014"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
