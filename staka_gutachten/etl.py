import logging
import os
import shutil
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
SITE_NAME = os.getenv("SHAREPOINT_SITE_NAME_STAKA_GUTACHTEN")
CERT_PATH = os.getenv("SHAREPOINT_CERT_PATH")
THUMBPRINT = os.getenv("SHAREPOINT_THUMBPRINT")
SHAREPOINT_FOLDER = "General"

DATA_ORIG_PATH = "data_orig"


def get_graph_token() -> str:
    """Authenticate against Microsoft Graph using certificate."""
    with open(CERT_PATH, "r") as f:
        private_key = f.read()

    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
        client_credential={"thumbprint": THUMBPRINT, "private_key": private_key},
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        raise RuntimeError(f"Auth failed: {result.get('error_description')}")
    return result["access_token"]


def get_site_id(token: str) -> str:
    """Resolve SharePoint site ID from hostname and site name."""
    url = f"https://graph.microsoft.com/v1.0/sites/{SHAREPOINT_HOST}:/sites/{SITE_NAME}"
    r = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    r.raise_for_status()
    return r.json()["id"]


def download_sharepoint_files(token: str, site_id: str, dest_dir: str):
    """Download all files from the SharePoint folder into dest_dir."""
    headers = {"Authorization": f"Bearer {token}"}

    # List files in the target folder
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives?$select=name,id"
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    drives = r.json()["value"]

    # Pick the default "Documents" drive (adjust name if your site uses another)
    drive = next((d for d in drives if d["name"] == "Documents"), drives[0])
    drive_id = drive["id"]

    # List items in the folder
    folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{SHAREPOINT_FOLDER}:/children"
    r = requests.get(folder_url, headers=headers)
    r.raise_for_status()
    items = r.json().get("value", [])

    for item in items:
        if "file" not in item:
            continue  # skip subfolders
        filename = item["name"]
        download_url = item["@microsoft.graph.downloadUrl"]
        dest_path = os.path.join(dest_dir, filename)
        logging.info(f"Downloading {filename} from SharePoint...")
        file_r = requests.get(download_url, stream=True)
        file_r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in file_r.iter_content(chunk_size=8192):
                f.write(chunk)


def sanitize_filename(name: str) -> str:
    transl_table = str.maketrans({"ä": "ae", "Ä": "Ae", "ö": "oe", "Ö": "Oe", "ü": "ue", "Ü": "Ue", "ß": "ss"})
    name = name.translate(transl_table).replace(" ", "_")
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
    return "".join(c for c in name if c in allowed)


def process_excel_file():
    excel_filename = "Liste_Gutachten.xlsx"
    excel_file_path = os.path.join(DATA_ORIG_PATH, excel_filename)
    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"The file '{excel_filename}' does not exist in the directory '{DATA_ORIG_PATH}'.")

    df = pd.read_excel(excel_file_path)
    df["Dateiname"] = df["Dateiname"].astype(str)
    df["Dateiname_ftp"] = df["Dateiname"].apply(sanitize_filename)

    def ensure_pdf_suffix(orig_name: str, ftp_name: str) -> str:
        if Path(orig_name).suffix.lower() == ".pdf" and Path(ftp_name).suffix.lower() != ".pdf":
            return str(Path(ftp_name).with_suffix(".pdf"))
        return ftp_name

    df["Dateiname_ftp"] = [ensure_pdf_suffix(o, f) for o, f in zip(df["Dateiname"], df["Dateiname_ftp"])]

    base_url = "https://data-bs.ch/stata/staka/gutachten/"
    gate_url = base_url + "index.html?file="
    df["URL_Datei"] = gate_url + df["Dateiname_ftp"]

    files_in_data_orig = set(os.listdir(DATA_ORIG_PATH))
    listed_files = set(df["Dateiname"])
    unlisted_files = files_in_data_orig - listed_files - {".gitkeep", "Liste_Gutachten.xlsx", "DESKTOP.INI"}
    if unlisted_files:
        raise ValueError(f"The following files are in 'data_orig' but not in 'Liste_Gutachten': {unlisted_files}")
    missing_files = listed_files - files_in_data_orig
    if missing_files:
        raise ValueError(
            f"The following files are listed in 'Liste_Gutachten' but do not exist in 'data_orig': {missing_files}"
        )

    logging.info("All files in 'data_orig' are listed in 'Liste_Gutachten' and vice versa.")
    return df


def upload_files_to_ftp(df: pd.DataFrame):
    remote_dir = "staka/gutachten/"
    os.makedirs("data", exist_ok=True)

    for orig_name, ftp_name in zip(df["Dateiname"], df["Dateiname_ftp"]):
        src_path = os.path.join(DATA_ORIG_PATH, orig_name)
        dst_path = os.path.join("data", ftp_name)
        shutil.copy2(src_path, dst_path)
        common.upload_ftp(dst_path, remote_path=remote_dir)
        logging.info(f"Uploaded {orig_name} as {ftp_name} to FTP at {remote_dir}")

    csv_filename = "100489_gutachten.csv"
    csv_file_path = os.path.join("data", csv_filename)
    df_out = df.drop(columns=["Dateiname_ftp"])
    df_out.to_csv(csv_file_path, index=False)
    common.update_ftp_and_odsp(csv_file_path, remote_dir, dataset_id="100489")


def main():
    token = get_graph_token()
    site_id = get_site_id(token)
    download_sharepoint_files(token, site_id, DATA_ORIG_PATH)

    df = process_excel_file()
    upload_files_to_ftp(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful.")
