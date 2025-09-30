import logging
import re
from datetime import datetime

import common
import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://www.unibas.ch/de/Studium/Vor-dem-Studium/Termine-Fristen/Semesterdaten.html"


def clean(s: str) -> str:
    """Normalize whitespace in a string."""
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_date(datestr: str) -> str:
    """Convert '23.12.2025' -> '2025-12-23'. If parsing fails, return empty string."""
    datestr = datestr.strip()
    if not datestr:
        return ""
    try:
        return datetime.strptime(datestr, "%d.%m.%Y").strftime("%Y-%m-%d")
    except Exception:
        logging.warning(f"Could not parse date: '{datestr}'")
        return ""


def split_dates(text: str):
    """
    Split a date range string like '23.12.2025–03.01.2026'.
    Returns (start_iso, end_iso) or ('','') if parsing fails.
    """
    if not text:
        return ("", "")
    text = text.replace("-", "-").replace("—", "-").replace("–", "-")
    parts = [p.strip() for p in text.split("-")]
    if len(parts) != 2:
        logging.warning(f"Could not split date range: '{text}'")
        return ("", "")
    return parse_date(parts[0]), parse_date(parts[1])


def split_semester(sem_text: str):
    """
    Split semester text like 'Frühjahrsemester 2025' -> ('Frühjahrsemester', '2025').
    If no year is found, return (sem_text, '').
    """
    parts = sem_text.strip().split()
    if len(parts) >= 2 and parts[-1].isdigit():
        return " ".join(parts[:-1]), parts[-1]
    return sem_text, ""


def parse_page(html: str) -> pd.DataFrame:
    """Parse Uni Basel semester tables and extract all rows with date ranges."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []

    for h3 in soup.find_all("h3"):
        sem_full = clean(h3.get_text())
        if not sem_full:
            continue
        sem_name, sem_year = split_semester(sem_full)

        tbl = h3.find_next("table")
        if not tbl:
            logging.warning(f"No table found for semester '{sem_full}'")
            continue

        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            label_raw = clean(tds[0].get_text(" "))
            value = clean(tds[1].get_text(" "))

            # Clean label (remove trailing colon or asterisk)
            name = label_raw.rstrip(":* ").strip()

            start, end = split_dates(value)
            if not (start or end):
                continue  # skip rows without valid date ranges

            rows.append(
                {
                    "semester": sem_name,
                    "jahr": sem_year,
                    "startdatum": start,
                    "enddatum": end,
                    "name": name,
                }
            )

    return pd.DataFrame(rows, columns=["semester", "jahr", "startdatum", "enddatum", "name"])


def main():
    out_csv = "data/100469_unibas_semesterdaten.csv"
    try:
        logging.info(f"Fetching data from {URL}")
        resp = requests.get(URL, timeout=30, headers={"User-Agent": "Mozilla/5.0 (HolidayScraper/1.0)"})
        resp.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch page: {e}")
        return

    df = parse_page(resp.text)
    if df.empty:
        logging.error("No data extracted from the page.")
        return

    # Save to CSV
    df.to_csv(out_csv, index=False, encoding="utf-8", sep=";")
    logging.info(f"Saved results to {out_csv}")

    # Upload to FTP
    common.update_ftp_and_odsp(path_export=out_csv, folder_name="ed/hochschulen", dataset_id="100469")
    logging.info("CSV-Datei wurde erfolgreich gespeichert: 100469_unibas_semesterdaten.csv")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=[logging.StreamHandler()]
    )
    main()

    logging.info("Job is successfully done.")
