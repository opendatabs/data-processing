import logging
import os
import re
import time
from contextlib import contextmanager
from typing import Dict, List, Tuple, Optional

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.remote_connection import LOGGER as SEL_LOGGER

import common

# ---------------------------
# Logging setup
# ---------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("geodaten_katalog")

# Quiet down very chatty libs
SEL_LOGGER.setLevel(logging.WARNING)
logging.getLogger("selenium").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

URL = "https://shop.geo.bs.ch/geodaten-katalog/"
HTML_SNAPSHOT = "Geodaten_Katalog.html"
EXPORT_DIR = "export"
DATA_DIR = "data"
CSV_NAME = "100410_geodatenkatalog.csv"
CSV_PATH = os.path.join(DATA_DIR, CSV_NAME)
CSV_EXPORT_PATH = os.path.join(EXPORT_DIR, CSV_NAME)
META_NAME = "gva_metadata.csv"
META_PATH = os.path.join(DATA_DIR, META_NAME)

# ---------------------------
# Helpers
# ---------------------------
@contextmanager
def elapsed(section: str):
    t0 = time.perf_counter()
    logger.info(f"▶ {section} — start")
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        logger.info(f"✅ {section} — done in {dt:.2f}s")


def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(EXPORT_DIR, exist_ok=True)
    logger.debug(f"Ensured directories: {DATA_DIR}, {EXPORT_DIR}")


def setup_driver() -> webdriver.Firefox:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    driver = webdriver.Firefox(options=options)
    # Hard timeouts to avoid “hang forever”
    driver.set_page_load_timeout(45)   # page load
    driver.set_script_timeout(30)      # async scripts
    logger.debug("Firefox WebDriver initialized with headless options.")
    return driver


def wait_for_catalog_loaded(driver: webdriver.Firefox, timeout: int = 30):
    # Wait until core elements are present
    logger.debug("Waiting for catalog headers to be present...")
    WebDriverWait(driver, timeout).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.headerText"))
    )
    # Optional: wait for sub-topic container(s)
    WebDriverWait(driver, timeout).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.SubGliedContext"))
    )
    logger.debug("Catalog content appears to be loaded.")


def fetch_page_source() -> str:
    with elapsed("Fetch page"):
        driver = setup_driver()
        try:
            logger.info(f"Navigating to {URL}")
            driver.get(URL)
            wait_for_catalog_loaded(driver, timeout=40)
            page_source = driver.page_source
            with open(HTML_SNAPSHOT, "w", encoding="utf-8") as f:
                f.write(page_source)
            logger.info(f"Saved HTML snapshot: {HTML_SNAPSHOT}")
            return page_source
        finally:
            driver.quit()
            logger.debug("WebDriver closed.")


def parse_catalog(page_source: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    with elapsed("Parse catalog HTML"):
        soup = BeautifulSoup(page_source, "html.parser")

        data: List[Dict] = []
        ebene_data: List[Dict] = []
        headers = soup.find_all("div", class_="headerText")
        logger.info(f"Found main topics: {len(headers)}")

        for i, header in enumerate(headers, start=1):
            main_theme = header.get_text(strip=True)
            logger.debug(f"[Header {i}] main topic: {main_theme}")

            header_container = header.find_parent("div", class_="header")
            sub_container = header_container.find_next_sibling("div", class_="SubGliedContext") if header_container else None
            if not sub_container:
                logger.warning(f"No sub-container for main topic '{main_theme}'. Adding empty row.")
                data.append(
                    {"Kategorie": main_theme, "Thema": None, "Abkuerzung": None, "Beschreibung": None,
                     "Aktualisierung": None, "Ebenen": None}
                )
                continue

            sub_themes = sub_container.find_all("div", id="thema")
            logger.info(f"  Sub-topics under '{main_theme}': {len(sub_themes)}")

            for j, sub_theme in enumerate(sub_themes, start=1):
                # ----- Title + Abbreviation
                raw_title_node = sub_theme.find("div", class_="themaTitel")
                if not raw_title_node:
                    logger.warning(f"    [Sub {j}] Missing themaTitel. Skipping.")
                    continue

                sub_theme_title = raw_title_node.get_text(strip=True)
                last_abbreviation: Optional[str] = None
                match = re.search(r"\(([^()]*)\)$", sub_theme_title)
                if match:
                    last_abbreviation = match.group(1)
                    sub_theme_title = re.sub(r"\s*\([^()]*\)$", "", sub_theme_title)

                # ----- Description
                description_div = sub_theme.find("div", class_="themaBesch")
                description = description_div.get_text(strip=True) if description_div else None

                # ----- Update date (two structure variants)
                date_div = sub_theme.find("div", class_="aktualisierung")
                update_date = date_div.get_text(strip=True).replace("Stand der Geodaten: ", "") if date_div else None
                if not update_date:
                    date_container = sub_theme.select_one("div.ebenen_date_container")
                    if date_container:
                        title_node = date_container.find(
                            "div",
                            class_="ebenen_dates_title",
                            string=lambda s: s and s.strip() == "Letzte Aktualisierung",
                        )
                        if title_node:
                            val_node = title_node.find_next_sibling("div")
                            if val_node:
                                update_date = val_node.get_text(strip=True)

                # ----- Links
                links_container = sub_theme.find("div", class_="themaLinksContainer")
                links_dict: Dict[str, str] = {}
                if links_container:
                    for link in links_container.find_all("a"):
                        link_text = link.get_text(strip=True)
                        href = link.get("href")
                        if link_text and href:
                            links_dict[link_text] = href

                # ----- Ebenen
                ebene_container = sub_theme.find_all("div", class_="ebenen")
                ebene_details: List[str] = []
                for ebene in ebene_container:
                    b = ebene.find("b")
                    title = b.get_text(strip=True) if b else None
                    beschreibung = b.next_sibling.strip() if (b and b.next_sibling) else None
                    if title:
                        ebene_details.append(title)
                        ebene_data.append({"Ebene": title, "Beschreibung": beschreibung})

                # ----- Image
                image_div = sub_theme.find("div", class_="themaBild")
                image_tag = image_div.find("img") if image_div else None
                image_url = (
                    f"https://shop.geo.bs.ch/geodaten-katalog/{image_tag['src']}"
                    if image_tag and image_tag.get("src")
                    else None
                )

                # ----- Assemble row
                entry = {
                    "Kategorie": main_theme,
                    "Thema": sub_theme_title,
                    "Abkuerzung": last_abbreviation,
                    "Beschreibung": description,
                    "Aktualisierung": update_date,
                    "Ebenen": " ; ".join(ebene_details) if ebene_details else None,
                    "Bild-URL": image_url,
                }
                for link_text, link_url in links_dict.items():
                    entry[link_text] = link_url

                data.append(entry)

        df = pd.DataFrame(data)
        df_ebenen = pd.DataFrame(ebene_data).drop_duplicates()

        logger.info(f"Parsed rows: topics={len(df):,}, ebene entries={len(df_ebenen):,}")
        return df, df_ebenen


def transform_and_save(df: pd.DataFrame, df_ebenen: pd.DataFrame) -> pd.DataFrame:
    with elapsed("Transform + save CSVs"):
        if df.empty:
            raise ValueError("Parsed DataFrame is empty. Aborting save.")

        # Map access columns if present
        if "öffentlich" in df.columns:
            df["öffentlich"] = df["öffentlich"].apply(lambda x: "Kategorie A" if pd.notna(x) else None)
        if "beschränkt öffentlich" in df.columns:
            df["beschränkt öffentlich"] = df["beschränkt öffentlich"].apply(
                lambda x: "Kategorie B" if pd.notna(x) else None
            )

        if ("öffentlich" in df.columns) or ("beschränkt öffentlich" in df.columns):
            df["Zugriff"] = df.get("öffentlich").fillna(df.get("beschränkt öffentlich"))
            for col in ("öffentlich", "beschränkt öffentlich"):
                if col in df.columns:
                    df.drop(columns=[col], inplace=True)
            df["Zugriff"] = df["Zugriff"] + ': "https://www.geo.bs.ch/erweiterte-berechtigung"'

        if "Abkuerzung" in df.columns:
            df["Page"] = "https://opendatabs.github.io/geoportal-poc/?param=" + df["Abkuerzung"].astype(str)

        desired_columns = [
            "Kategorie",
            "Thema",
            "Abkuerzung",
            "Page",
            "Beschreibung",
            "Aktualisierung",
            "Geodaten-Shop",
            "Metadaten",
            "MapBS",
            "Geobasisdaten",
            "Ebenen",
            "WMS",
            "WFS",
            "WMTS",
            "Bild-URL",
            "Zugriff",
        ]
        existing_columns = [c for c in desired_columns if c in df.columns]
        if not existing_columns:
            logger.warning("No desired columns found; saving all available columns.")
            save_df = df
        else:
            save_df = df[existing_columns]

        ensure_dirs()
        save_df.to_csv(CSV_PATH, index=False, sep=";")
        save_df.to_csv(CSV_EXPORT_PATH, index=False, sep=";")
        logger.info(f"Saved CSV rows: {len(save_df):,} → {CSV_PATH} and {CSV_EXPORT_PATH}")

        # Side-effect: FTP + ODS update
        try:
            common.update_ftp_and_odsp(CSV_PATH, "/gva/geodatenkatalog", "100410")
            logger.info("FTP/ODS update completed.")
        except Exception as e:
            logger.error(f"FTP/ODS update failed: {e}")

        # Build metadata CSV
        meta_df = build_metadata(save_df, df_ebenen)
        meta_df.to_csv(META_PATH, index=False, sep=";")
        logger.info(f"Saved metadata rows: {len(meta_df):,} → {META_PATH}")

        return save_df


def build_metadata(df: pd.DataFrame, df_ebenen: pd.DataFrame) -> pd.DataFrame:
    with elapsed("Build metadata"):
        # Group by Thema, merge categories, take first of others
        group_aggs = {"Kategorie": lambda x: ";".join(sorted(set([str(v) for v in x if pd.notna(v)])))}
        for col in df.columns:
            if col not in ["Thema", "Kategorie"]:
                group_aggs[col] = "first"

        grouped_df = df.groupby("Thema", dropna=False).agg(group_aggs).reset_index()

        grouped_df["title"] = grouped_df["Thema"]
        grouped_df["modified"] = grouped_df.get("Aktualisierung")
        grouped_df["attributions"] = "Geodaten Kanton Basel-Stadt"
        grouped_df["language"] = "de"
        grouped_df["tags"] = grouped_df.get("Kategorie")

        def create_description(row) -> str:
            parts = []
            parts.append("<div style='display: flex; align-items: flex-start; gap: 20px;'>")
            parts.append("<div style='flex: 1;'>")

            # Kategorie (linked)
            parts.append("<h3>Kategorie</h3><ul>")
            cats = (row.get("Kategorie") or "")
            for category in [c.strip() for c in cats.split(";") if c and c.strip()]:
                url = f"https://data.bs.ch/explore/?q=tags%3D{category.replace(' ', '%20')}"
                parts.append(f"<li><a href='{url}' target='_blank'>{category}</a></li>")
            parts.append("</ul>")

            # Beschreibung
            beschr = row.get("Beschreibung") or ""
            parts.append("<h3>Beschreibung</h3>")
            parts.append(f"<p>{beschr}</p>")

            # Ebenen with descriptions
            parts.append("<h3>Ebenen</h3><ul>")
            ebenen = row.get("Ebenen") or ""
            for ebene in [e.strip() for e in ebenen.split(";") if e and e.strip()]:
                beschreibungs_val = df_ebenen.loc[df_ebenen["Ebene"] == ebene, "Beschreibung"].values
                if len(beschreibungs_val) > 0 and str(beschreibungs_val[0]).strip():
                    parts.append(f"<li><strong>{ebene}:</strong> {beschreibungs_val[0]}</li>")
                else:
                    parts.append(f"<li>{ebene}</li>")
            parts.append("</ul>")

            # Links
            link_cols = ["Geodaten-Shop", "Metadaten", "MapBS", "Geobasisdaten", "WMS", "WFS", "WMTS"]
            parts.append("<h3>Links</h3><ul>")
            for col in link_cols:
                href = row.get(col)
                if pd.notna(href):
                    parts.append(f"<li><a href='{href}' target='_blank'>{col}</a></li>")
            parts.append("</ul></div>")

            # Bild
            img = row.get("Bild-URL")
            if pd.notna(img):
                parts.append(
                    f"<div><img src='{img}' alt='Bildbeschreibung' "
                    f"style='max-width:300px; height:auto; border-radius:8px;'></div>"
                )

            parts.append("</div>")
            return "".join(parts)

        grouped_df["description"] = grouped_df.apply(create_description, axis=1)
        final_df = grouped_df[["title", "description", "attributions", "modified", "tags", "language"]]
        return final_df


def main():
    with elapsed("Main pipeline"):
        try:
            page = fetch_page_source()
            df, df_ebenen = parse_catalog(page)
            transform_and_save(df, df_ebenen)
        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()
