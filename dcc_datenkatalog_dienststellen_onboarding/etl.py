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

SOURCE_LOCAL_NAME = "Uebersichtsliste Dienststellen und Data Owner.xlsx"
SHAREPOINT_FILE_PATH = f"Datenkatalog/1-Dienststellen/{SOURCE_LOCAL_NAME}"

DATA_ORIG_DIR = Path("data_orig")
SOURCE_FILE = DATA_ORIG_DIR / SOURCE_LOCAL_NAME
OUTPUT_DIR = Path("data")
ODS_DATASET_ID = "100537"
FTP_REMOTE_PATH = "dcc/datenkatalog"
OUTPUT_FILE = OUTPUT_DIR / f"{ODS_DATASET_ID}_datenkatalog_dienststellen_onboarding.csv"

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

    drive = next(
        (d for d in drives if d["name"] == "Documents"),
        drives[0],
    )

    return drive["id"]


def download_file(
    token: str,
    drive_id: str,
    sharepoint_path: str,
    dest_path: Path,
) -> None:
    """Download a single file from SharePoint to ``dest_path``."""
    headers = {"Authorization": f"Bearer {token}"}

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{sharepoint_path}"

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    download_url = r.json()["@microsoft.graph.downloadUrl"]

    logging.info("Downloading %s", sharepoint_path)

    file_r = requests.get(download_url, stream=True)
    file_r.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in file_r.iter_content(chunk_size=8192):
            f.write(chunk)


def download_sharepoint_file(token: str, site_id: str) -> None:
    drive_id = get_drive_id(token, site_id)
    download_file(
        token=token,
        drive_id=drive_id,
        sharepoint_path=SHAREPOINT_FILE_PATH,
        dest_path=SOURCE_FILE,
    )


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
