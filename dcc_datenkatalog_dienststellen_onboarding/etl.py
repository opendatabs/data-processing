import logging
import os
from pathlib import Path

import common
import msal
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_HOST = os.getenv("SHAREPOINT_HOST")
SITE_NAME = os.getenv("SHAREPOINT_SITE_NAME_DCC")
CERT_PATH = os.getenv("SHAREPOINT_CERT_PATH")
THUMBPRINT = os.getenv("SHAREPOINT_THUMBPRINT")

SOURCE_LOCAL_NAME = "Übersichtsliste Dienststellen und Data Owner.xlsx"
SHAREPOINT_FOLDER = "Datenkatalog/1-Dienststellen"

DATA_ORIG_DIR = Path("data_orig")
SOURCE_FILE = DATA_ORIG_DIR / SOURCE_LOCAL_NAME
OUTPUT_DIR = Path("data")
ODS_DATASET_ID = "100537"
FTP_REMOTE_PATH = "dcc/datenkatalog"
OUTPUT_FILE = OUTPUT_DIR / f"{ODS_DATASET_ID}_datenkatalog_dienststellen_onboarding.csv"

# Prefer the shared documents library; German sites often use these names.
DRIVE_NAME_CANDIDATES = ("Documents", "Dokumente", "Freigegebene Dokumente")

SOURCE_COLUMNS = [
    "Departement",
    "Posten ",
    "Status: Kontaktiert",
    "Status: Info",
    "Status: Kick-Off",
    "Status: Metadatenerfassung",
    "Status: Review und Abnahme",
    "Status: Abgeschlossen",
]

OUTPUT_COLUMNS = [
    "Departement",
    "Posten",
    "Status: Kontaktiert",
    "Status: Info",
    "Status: Kick-Off",
    "Status: Metadatenerfassung",
    "Status: Review und Abnahme",
    "Status: Abgeschlossen",
]

STATUS_COLUMNS = OUTPUT_COLUMNS[2:]


def get_graph_token() -> str:
    with open(CERT_PATH, "r") as f:
        private_key = f.read()

    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential={
            "thumbprint": THUMBPRINT,
            "private_key": private_key,
        },
    )

    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result.get('error_description')}")

    return result["access_token"]


def get_site_id(token: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_HOST}:/sites/{SITE_NAME}"

    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()

    return r.json()["id"]


def get_drive_id(token: str, site_id: str) -> str:
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives?$select=name,id"

    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
    )
    r.raise_for_status()

    drives = r.json()["value"]
    drive_names = [d["name"] for d in drives]
    logging.info("Available SharePoint drives: %s", drive_names)

    for candidate in DRIVE_NAME_CANDIDATES:
        drive = next((d for d in drives if d["name"] == candidate), None)
        if drive is not None:
            logging.info("Using SharePoint drive '%s'", drive["name"])
            return drive["id"]

    if not drives:
        raise RuntimeError(f"No drives found for site {site_id}")

    logging.warning("No preferred drive found; falling back to '%s'", drives[0]["name"])
    return drives[0]["id"]


def _normalize_filename(name: str) -> str:
    """Compare filenames ignoring umlaut spelling variants and case."""
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "Ä": "Ae",
        "Ö": "Oe",
        "Ü": "Ue",
        "ß": "ss",
    }
    normalized = name
    for src, dst in replacements.items():
        normalized = normalized.replace(src, dst)
    return normalized.casefold().strip()


def _list_folder_children(token: str, drive_id: str, folder_path: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}:/children"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json().get("value", [])


def download_sharepoint_file(token: str, site_id: str) -> None:
    """Download the Excel by listing the folder, then using the item download URL.

    Path-based Graph lookups for this filename returned 404 even with correct
    percent-encoding; resolving via folder children is more reliable.
    """
    drive_id = get_drive_id(token, site_id)
    items = _list_folder_children(token, drive_id, SHAREPOINT_FOLDER)
    item_names = [item.get("name") for item in items]
    logging.info("Files in SharePoint folder '%s': %s", SHAREPOINT_FOLDER, item_names)

    target_normalized = _normalize_filename(SOURCE_LOCAL_NAME)
    match = next(
        (
            item
            for item in items
            if "file" in item and _normalize_filename(item.get("name", "")) == target_normalized
        ),
        None,
    )
    if match is None:
        raise FileNotFoundError(
            f"File '{SOURCE_LOCAL_NAME}' not found in SharePoint folder '{SHAREPOINT_FOLDER}'. "
            f"Available items: {item_names}"
        )

    download_url = match["@microsoft.graph.downloadUrl"]
    SOURCE_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.info("Downloading %s/%s", SHAREPOINT_FOLDER, match["name"])

    file_r = requests.get(download_url, stream=True)
    file_r.raise_for_status()
    with open(SOURCE_FILE, "wb") as f:
        for chunk in file_r.iter_content(chunk_size=8192):
            f.write(chunk)


def fetch_source_file() -> Path:
    """Download the source Excel from SharePoint, falling back to the local copy."""
    try:
        token = get_graph_token()
        site_id = get_site_id(token)
        download_sharepoint_file(token, site_id)
    except Exception:
        logging.exception("SharePoint download failed. Falling back to local file in %s", DATA_ORIG_DIR)
        if not SOURCE_FILE.exists():
            raise FileNotFoundError(f"SharePoint download failed and no local fallback file found: {SOURCE_FILE}")
        logging.warning("Using existing local source file %s", SOURCE_FILE)

    return SOURCE_FILE


def _format_status_dates(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()
    for column in STATUS_COLUMNS:
        formatted[column] = pd.to_datetime(formatted[column], errors="coerce").dt.strftime("%Y-%m-%d")
        formatted[column] = formatted[column].fillna("")
    return formatted


def _drop_rows_without_status(df: pd.DataFrame) -> pd.DataFrame:
    has_status = df[STATUS_COLUMNS].fillna("").astype(str).apply(lambda col: col.str.strip()).ne("").any(axis=1)
    return df[has_status].reset_index(drop=True)


def _extract_onboarding_df(source_path: Path) -> pd.DataFrame:
    source_df = pd.read_excel(source_path)
    missing = [column for column in SOURCE_COLUMNS if column not in source_df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in {source_path.name}: {missing}")

    df = source_df[SOURCE_COLUMNS].rename(columns={"Posten ": "Posten"})
    df["Posten"] = df["Posten"].str.removeprefix("Data Owner ")
    df = _format_status_dates(df)[OUTPUT_COLUMNS]
    return _drop_rows_without_status(df)


def main() -> None:
    """Extract onboarding status columns from the source Excel file."""
    logging.info("ETL job started")

    source_path = fetch_source_file()
    logging.info("Reading source data from %s", source_path.name)
    df = _extract_onboarding_df(source_path)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    logging.info("Wrote %s (%d rows)", OUTPUT_FILE, len(df))

    common.update_ftp_and_odsp(str(OUTPUT_FILE), FTP_REMOTE_PATH, ODS_DATASET_ID)

    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Executing %s...", __file__)
    main()
    logging.info("Job successful.")
