import logging
import os
from io import StringIO
from typing import Dict, Iterable, List
from urllib.parse import urljoin

import common
import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.bs.ch"
START_URL = (
    "https://www.bs.ch/regierungsrat/staatskanzlei/oeffentlichkeitsprinzip/verzeichnis-der-verfahren-mit-personendaten"
)


def fetch_html(url: str) -> str:
    """Fetch a URL and return the HTML text."""
    logging.debug("Fetching URL: %s", url)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


def iter_departments_and_abteilungen(html: str) -> Iterable[Dict[str, str]]:
    """Parse the index page and yield (department, abteilung, url) dicts.

    This is tailored to the structure of the official page:
    https://www.bs.ch/regierungsrat/staatskanzlei/oeffentlichkeitsprinzip/verzeichnis-der-verfahren-mit-personendaten
    """
    soup = BeautifulSoup(html, "lxml")

    # List of h2 sections that correspond to the actual
    # departments/areas listed on the official page.
    allowed_departments = {
        "Präsidialdepartement",
        "Bau- und Verkehrsdepartement",
        "Erziehungsdepartement",
        "Finanzdepartement",
        "Gesundheitsdepartement",
        "Justiz- und Sicherheitsdepartement",
        "Departement für Wirtschaft, Soziales und Umwelt",
        "Datenschutzbeauftragter",
        "Finanzkontrolle",
        "Gerichte",
        "Staatsanwaltschaft",
        "Ombudsstelle",
    }

    # Each department is introduced by an h2; beneath it we have bullet
    # points (li) for the abteilungen, containing links to the procedures.
    for h2 in soup.find_all("h2"):
        department = h2.get_text(strip=True)
        if not department or department not in allowed_departments:
            continue

        # Walk siblings until the next h2 (or end), looking for list items.
        for sibling in h2.find_all_next():
            if sibling is h2:
                # skip the header itself
                continue
            if sibling.name == "h2":
                # reached the next department
                break
            if sibling.name != "li":
                continue

            li = sibling
            link = li.find("a", href=True)
            if not link:
                continue

            full_text = li.get_text(" ", strip=True)
            # The text pattern is typically:
            # "Aussenbeziehungen und Standortmarketing Hier finden Sie die Verfahren ..."
            split_marker = " Hier finden Sie"
            if split_marker in full_text:
                abteilung = full_text.split(split_marker, 1)[0].strip()
            else:
                # Fallback: use the link text or the full list item text
                abteilung = link.get_text(strip=True) or full_text

            url = urljoin(BASE_URL, link["href"])
            yield {
                "department": department,
                "abteilung": abteilung,
                "url": url,
            }


def extract_tables_for_entry(entry: Dict[str, str]) -> List[pd.DataFrame]:
    """Fetch a single abteilung page and return all tables as DataFrames.

    Each returned DataFrame is enriched with metadata columns.
    """
    url = entry["url"]
    logging.info(
        "Scraping tables for department='%s', abteilung='%s' from %s",
        entry["department"],
        entry["abteilung"],
        url,
    )

    # Fetch full HTML so we can both parse tables and discover in-page
    # anchors from the "Auf dieser Seite" table of contents.
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    # Collect anchors from the "Auf dieser Seite" section, if present.
    # These have hrefs like "#verfahren-xyz" that can be appended to
    # the page URL to link directly to the corresponding section/table.
    fragments: List[str] = []
    toc_heading = soup.find(
        lambda tag: tag.name in {"h2", "h3", "h4", "p", "div"} and tag.get_text(strip=True) == "Auf dieser Seite"
    )
    if toc_heading is not None:
        ul = toc_heading.find_next("ul")
        if ul is not None:
            for a in ul.find_all("a", href=True):
                # Skip the "Übersicht" link as it's not an actual entry
                link_text = a.get_text(strip=True)
                if link_text == "Übersicht über die Verfahren mit Personendaten":
                    continue
                href = a["href"]
                if href.startswith("#"):
                    fragments.append(href)

    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        # No tables found on this page
        logging.warning("No tables found on %s", url)
        return []

    html_tables = soup.find_all("table")

    # Expected field names in order (always 6 rows)
    expected_fields = [
        "Bezeichnung",
        "Rechtsgrundlage(n)",
        "Quelle(n)",
        "Verantwortliche Stelle",
        "Internetauftritt",
        "Zweck der Datenbearbeitung",
    ]

    enriched: List[pd.DataFrame] = []
    for idx, df in enumerate(tables):
        df = df.copy()

        # Tables are always 2 columns: first = field name, second = value.
        # We expect exactly 6 rows in the specified order.
        if df.shape[1] >= 2 and df.shape[0] >= 6:
            # Extract values from the second column for each expected field
            values = {}
            for i, field_name in enumerate(expected_fields):
                if i < len(df):
                    # Get the value from the second column (index 1)
                    value = df.iloc[i, 1] if df.shape[1] > 1 else ""
                    values[field_name] = value
                else:
                    values[field_name] = ""

            # Extract and parse multi-valued fields from raw HTML
            # Use | as delimiter for multi-valued entries (Mehrwertig)
            MULTI_VALUE_DELIMITER = "|"

            if idx < len(html_tables):
                html_table = html_tables[idx]
                # Get all rows, excluding header rows in thead
                tbody = html_table.find("tbody")
                if tbody:
                    rows = tbody.find_all("tr")
                else:
                    # If no tbody, get all tr but skip thead rows
                    rows = html_table.find_all("tr")
                    # Remove rows that are in thead
                    thead = html_table.find("thead")
                    if thead:
                        header_rows = set(thead.find_all("tr"))
                        rows = [r for r in rows if r not in header_rows]

                # Process Rechtsgrundlage(n) - may contain multiple entries
                rechtsgrundlage_idx = expected_fields.index("Rechtsgrundlage(n)")
                if rechtsgrundlage_idx < len(rows):
                    row = rows[rechtsgrundlage_idx]
                    cells = row.find_all(["td", "th"])
                    if len(cells) > 1:
                        value_cell = cells[1]  # Second column
                        # Extract text from paragraphs or list items, each as separate entry
                        entries = []
                        # Check for paragraphs
                        paragraphs = value_cell.find_all("p")
                        if paragraphs:
                            for p in paragraphs:
                                text = p.get_text(strip=True)
                                if text:
                                    entries.append(text)
                        else:
                            # Check for list items
                            list_items = value_cell.find_all("li")
                            if list_items:
                                for li in list_items:
                                    text = li.get_text(strip=True)
                                    if text:
                                        entries.append(text)
                            else:
                                # Fallback: split by line breaks or use whole text
                                text = value_cell.get_text(separator="\n", strip=True)
                                if text:
                                    # Split by newlines and filter empty
                                    lines = [line.strip() for line in text.split("\n") if line.strip()]
                                    if lines:
                                        entries = lines
                        if entries:
                            values["Rechtsgrundlage(n)"] = MULTI_VALUE_DELIMITER.join(entries)

                # Process Quelle(n) - extract hrefs, may have multiple entries
                quelle_idx = expected_fields.index("Quelle(n)")
                if quelle_idx < len(rows):
                    row = rows[quelle_idx]
                    cells = row.find_all(["td", "th"])
                    if len(cells) > 1:
                        value_cell = cells[1]  # Second column
                        # Find all links in this cell
                        links = value_cell.find_all("a", href=True)
                        if links:
                            hrefs = []
                            for link in links:
                                href = link.get("href", "")
                                if href:
                                    # Make absolute URL if relative
                                    if href.startswith("http"):
                                        hrefs.append(href)
                                    elif href.startswith("/"):
                                        hrefs.append(urljoin(BASE_URL, href))
                                    else:
                                        # Relative URL, make absolute based on current page
                                        hrefs.append(urljoin(url, href))
                            if hrefs:
                                values["Quelle(n)"] = MULTI_VALUE_DELIMITER.join(hrefs)

                # Process Internetauftritt - extract hrefs
                internetauftritt_idx = expected_fields.index("Internetauftritt")
                if internetauftritt_idx < len(rows):
                    row = rows[internetauftritt_idx]
                    cells = row.find_all(["td", "th"])
                    if len(cells) > 1:
                        value_cell = cells[1]  # Second column
                        # Find all links in this cell
                        links = value_cell.find_all("a", href=True)
                        if links:
                            hrefs = []
                            for link in links:
                                href = link.get("href", "")
                                if href:
                                    # Make absolute URL if relative
                                    if href.startswith("http"):
                                        hrefs.append(href)
                                    elif href.startswith("/"):
                                        hrefs.append(urljoin(BASE_URL, href))
                                    else:
                                        # Relative URL, make absolute based on current page
                                        hrefs.append(urljoin(url, href))
                            if hrefs:
                                values["Internetauftritt"] = MULTI_VALUE_DELIMITER.join(hrefs)

            # Create a single-row dataframe with the fixed column names
            wide = pd.DataFrame([values])
        else:
            # Fallback: if table structure differs, create empty row with expected columns
            wide = pd.DataFrame([{field: "" for field in expected_fields}])

        # Decide the most specific URL we can use for this table: if a
        # fragment from "Auf dieser Seite" exists at this index, append
        # it so consumers can jump directly to the corresponding section.
        fragment = fragments[idx] if idx < len(fragments) else ""
        source_url = f"{url}{fragment}"

        # Add metadata columns at the front
        wide.insert(0, "table_index", idx)
        wide.insert(0, "source_url", source_url)
        wide.insert(0, "abteilung", entry["abteilung"])
        wide.insert(0, "department", entry["department"])
        enriched.append(wide)

    return enriched


def main() -> None:
    """Run the scraping ETL and write the combined table CSV.

    Returns the path to the written CSV file.
    """
    logging.info("Starting ETL for Basel-Stadt procedures directory")
    index_html = fetch_html(START_URL)

    entries = list[Dict[str, str]](iter_departments_and_abteilungen(index_html))
    logging.info("Found %d department/abteilung pages to scrape", len(entries))

    all_tables: List[pd.DataFrame] = []
    for entry in entries:
        all_tables.extend(extract_tables_for_entry(entry))

    if not all_tables:
        raise RuntimeError("No tables could be extracted from any department/abteilung page.")

    combined = pd.concat(all_tables, ignore_index=True, sort=False)

    # 1. Set abteilung to NaN if it starts with "Verzeichnis der Verfahren mit Personendaten"
    mask = combined["abteilung"].astype(str).str.startswith("Verzeichnis der Verfahren mit Personendaten")
    combined.loc[mask, "abteilung"] = pd.NA

    # 2. Create a new column joining "department/abteilung/Verantwortliche Stelle" with "/"
    # Handle NaN values by converting to empty string for joining
    def join_path(row):
        parts = []
        dept = str(row["department"]) if pd.notna(row["department"]) else ""
        abt = str(row["abteilung"]) if pd.notna(row["abteilung"]) else ""
        stelle = str(row["Verantwortliche Stelle"]) if pd.notna(row["Verantwortliche Stelle"]) else ""

        if dept:
            parts.append(dept)
        if abt:
            parts.append(abt)
        if stelle:
            parts.append(stelle)

        return "/".join(parts) if parts else ""

    combined["path"] = combined.apply(join_path, axis=1)

    os.makedirs("data", exist_ok=True)
    output_path = os.path.join("data", "100520_staka_verz_verf_persdat.csv")
    combined.to_csv(output_path, index=False)
    common.update_ftp_and_odsp(output_path, "staka/verzeichnis_verfahren_personendaten", "100520")

    logging.info("ETL completed successfully, wrote %d rows to %s", len(combined), output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Executing %s...", __file__)
    main()
    logging.info("Job successful.")
