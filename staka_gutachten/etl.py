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

SHAREPOINT_ROOT = "General"

DATA_ORIG_PATH = "data_orig"

DEPARTEMENTS = ["BVD", "ED", "FD", "GD", "JSD", "PD", "WSU"]


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

    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )

    if "access_token" not in result:
        raise RuntimeError(
            f"Auth failed: {result.get('error_description')}"
        )

    return result["access_token"]


def get_site_id(token: str) -> str:
    url = (
        f"https://graph.microsoft.com/v1.0/sites/"
        f"{SHAREPOINT_HOST}:/sites/{SITE_NAME}"
    )

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


def download_folder(
    token: str,
    drive_id: str,
    sharepoint_folder: str,
    local_dir: str,
):
    """
    Download all files from a SharePoint folder recursively.
    """

    headers = {"Authorization": f"Bearer {token}"}

    os.makedirs(local_dir, exist_ok=True)

    url = (
        f"https://graph.microsoft.com/v1.0/drives/"
        f"{drive_id}/root:/{sharepoint_folder}:/children"
    )

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    items = r.json().get("value", [])

    for item in items:
        name = item["name"]

        # Folder
        if "folder" in item:
            sub_sp_path = f"{sharepoint_folder}/{name}"
            sub_local_dir = os.path.join(local_dir, name)

            download_folder(
                token,
                drive_id,
                sub_sp_path,
                sub_local_dir,
            )

            continue

        # File
        if "file" not in item:
            continue

        download_url = item["@microsoft.graph.downloadUrl"]

        dest_path = os.path.join(local_dir, name)

        logging.info(f"Downloading {sharepoint_folder}/{name}")

        file_r = requests.get(download_url, stream=True)
        file_r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in file_r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_sharepoint_files(token: str, site_id: str):
    """
    Download:
    - Excel-Datei/Liste_Gutachten.xlsx
    - Gutachten/<Departement>/*.pdf
    """

    drive_id = get_drive_id(token, site_id)

    os.makedirs(DATA_ORIG_PATH, exist_ok=True)

    # ------------------------------------------------------------------
    # Download Excel file
    # ------------------------------------------------------------------

    download_folder(
        token=token,
        drive_id=drive_id,
        sharepoint_folder=f"{SHAREPOINT_ROOT}/Excel-Datei",
        local_dir=DATA_ORIG_PATH,
    )

    # ------------------------------------------------------------------
    # Download Gutachten PDFs
    # ------------------------------------------------------------------

    for departement in DEPARTEMENTS:
        download_folder(
            token=token,
            drive_id=drive_id,
            sharepoint_folder=(
                f"{SHAREPOINT_ROOT}/Gutachten/{departement}"
            ),
            local_dir=DATA_ORIG_PATH,
        )


def sanitize_filename(name: str) -> str:
    transl_table = str.maketrans(
        {
            "ä": "ae",
            "Ä": "Ae",
            "ö": "oe",
            "Ö": "Oe",
            "ü": "ue",
            "Ü": "Ue",
            "ß": "ss",
        }
    )

    name = name.translate(transl_table).replace(" ", "_")

    allowed = (
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789._-"
    )

    return "".join(c for c in name if c in allowed)


def process_excel_file():
    excel_filename = "Liste_Gutachten.xlsx"

    excel_file_path = os.path.join(
        DATA_ORIG_PATH,
        excel_filename,
    )

    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(
            f"The file '{excel_filename}' does not exist "
            f"in '{DATA_ORIG_PATH}'."
        )

    df = pd.read_excel(excel_file_path)

    df["Dateiname"] = (
        df["Dateiname"]
        .astype(str)
        .str.strip()
    )

    def ensure_pdf_name(name: str) -> str:
        return name if Path(name).suffix else f"{name}.pdf"

    df["Dateiname"] = df["Dateiname"].apply(
        ensure_pdf_name
    )

    df["Dateiname_ftp"] = df["Dateiname"].apply(
        sanitize_filename
    )

    def ensure_pdf_suffix(
        orig_name: str,
        ftp_name: str,
    ) -> str:
        if (
            Path(orig_name).suffix.lower() == ".pdf"
            and Path(ftp_name).suffix.lower() != ".pdf"
        ):
            return str(
                Path(ftp_name).with_suffix(".pdf")
            )

        return ftp_name

    df["Dateiname_ftp"] = [
        ensure_pdf_suffix(o, f)
        for o, f in zip(
            df["Dateiname"],
            df["Dateiname_ftp"],
        )
    ]

    base_url = "https://data-bs.ch/stata/staka/gutachten/"
    gate_url = base_url + "index.html?file="

    df["URL_Datei"] = gate_url + df["Dateiname_ftp"]

    files_in_data_orig = {
        f
        for f in os.listdir(DATA_ORIG_PATH)
        if os.path.isfile(os.path.join(DATA_ORIG_PATH, f))
    }

    listed_files = set(df["Dateiname"])

    ignored = {
        ".gitkeep",
        "Liste_Gutachten.xlsx",
        "DESKTOP.INI",
    }

    unlisted_files = (
        files_in_data_orig
        - listed_files
        - ignored
    )

    if unlisted_files:
        raise ValueError(
            "The following files are in 'data_orig' "
            f"but not in 'Liste_Gutachten': {unlisted_files}"
        )

    missing_files = listed_files - files_in_data_orig

    if missing_files:
        raise ValueError(
            "The following files are listed in "
            "'Liste_Gutachten' but do not exist "
            f"in 'data_orig': {missing_files}"
        )

    logging.info(
        "All files in 'data_orig' are listed "
        "in 'Liste_Gutachten' and vice versa."
    )

    return df


def upload_files_to_ftp(df: pd.DataFrame):
    remote_dir = "staka/gutachten/"

    os.makedirs("data", exist_ok=True)

    for orig_name, ftp_name in zip(
        df["Dateiname"],
        df["Dateiname_ftp"],
    ):
        src_path = os.path.join(
            DATA_ORIG_PATH,
            orig_name,
        )

        dst_path = os.path.join(
            "data",
            ftp_name,
        )

        shutil.copy2(src_path, dst_path)

        common.upload_ftp(
            dst_path,
            remote_path=remote_dir,
        )

        logging.info(
            f"Uploaded {orig_name} as {ftp_name}"
        )

    csv_filename = "100489_gutachten.csv"

    csv_file_path = os.path.join(
        "data",
        csv_filename,
    )

    df_out = df.drop(columns=["Dateiname_ftp"])

    df_out.to_csv(csv_file_path, index=False)

    common.update_ftp_and_odsp(
        csv_file_path,
        remote_dir,
        dataset_id="100489",
    )


def main():
    token = get_graph_token()

    site_id = get_site_id(token)

    download_sharepoint_files(token, site_id)

    df = process_excel_file()

    upload_files_to_ftp(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    main()

    logging.info("Job successful.")
