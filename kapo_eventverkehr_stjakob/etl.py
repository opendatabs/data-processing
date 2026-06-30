import logging
import os
import re

import common
import markdown
import msal
import pandas as pd
import requests
from dotenv import load_dotenv
from markdown_newtab import NewTabExtension

load_dotenv()

TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_HOST = os.getenv("SHAREPOINT_HOST")
SITE_NAME = os.getenv("SHAREPOINT_SITE_NAME_KAPO_EVENTVERKEHR_STJAKOB")
CERT_PATH = os.getenv("SHAREPOINT_CERT_PATH")
THUMBPRINT = os.getenv("SHAREPOINT_THUMBPRINT")

SHAREPOINT_ROOT = "Eventliste"
DATA_ORIG_PATH = "data_orig"
EXCEL_FILENAME = "Events St. Jakob.xlsx"


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


def download_folder(
    token: str,
    drive_id: str,
    sharepoint_folder: str,
    local_dir: str,
):
    """Download all files from a SharePoint folder recursively."""
    headers = {"Authorization": f"Bearer {token}"}

    os.makedirs(local_dir, exist_ok=True)

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{sharepoint_folder}:/children"

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    items = r.json().get("value", [])

    for item in items:
        name = item["name"]

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

        if "file" not in item:
            continue

        download_url = item["@microsoft.graph.downloadUrl"]
        dest_path = os.path.join(local_dir, name)

        logging.info("Downloading %s/%s", sharepoint_folder, name)

        file_r = requests.get(download_url, stream=True)
        file_r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in file_r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_sharepoint_files(token: str, site_id: str):
    """Download Excel file and PNG images from SharePoint into data_orig."""
    drive_id = get_drive_id(token, site_id)

    download_folder(
        token=token,
        drive_id=drive_id,
        sharepoint_folder=SHAREPOINT_ROOT,
        local_dir=DATA_ORIG_PATH,
    )


def split_markdown_image(md):
    """Extract alt text and image source from markdown image syntax."""
    match = re.match(r"!\[(.*?)\]\((.*?)\)", str(md).strip())
    if match:
        return match.group(1), match.group(2)
    return None, None


def split_markdown_links(md):
    """Extract display text and URLs from markdown link syntax, handling multiple links."""
    parts = [p.strip() for p in str(md).split(";") if p.strip()]
    anzeigetexte, links = [], []
    for p in parts:
        match = re.match(r"\[(.*?)\]\((.*?)\)", p)
        if match:
            anzeigetexte.append(match.group(1))
            links.append(match.group(2))
        else:
            anzeigetexte.append("")
            links.append("")
    return ";".join(anzeigetexte), ";".join(links)


def process_data():
    file_path = os.path.join(DATA_ORIG_PATH, EXCEL_FILENAME)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{EXCEL_FILENAME}' does not exist in '{DATA_ORIG_PATH}'.")

    # Read the Sheet with the event data
    df_eventliste = pd.read_excel(file_path, sheet_name="Eventliste")
    df_eventliste["Info_Text_HTML"] = df_eventliste["Info_Text"].apply(
        lambda x: markdown.markdown(x, extensions=["nl2br", NewTabExtension()]) if pd.notna(x) else x
    )
    # Read the Sheet with the "Anreiseempfehlung" data
    df_anreiseempf = pd.read_excel(file_path, sheet_name="Anreiseempfehlung")
    df_anreiseempf["Text_HTML"] = df_anreiseempf["Text"].apply(
        lambda x: markdown.markdown(x, extensions=["nl2br", NewTabExtension()]) if pd.notna(x) else x
    )
    df_anreiseempf[["Alt-Texte", "Bildquellen"]] = df_anreiseempf["Bilder"].apply(
        lambda x: pd.Series(split_markdown_image(x))
    )

    df_anreiseempf[["Link_Anzeigetexte", "Links"]] = df_anreiseempf["Weiterfuehrende Links"].apply(
        lambda x: pd.Series(split_markdown_links(x))
    )
    df_zeitraum_info = pd.read_excel(file_path, sheet_name="Zeitraum_Info")
    df_zeitraum_info["Text_HTML"] = df_zeitraum_info["Text"].apply(
        lambda x: markdown.markdown(x, extensions=["nl2br", NewTabExtension()]) if pd.notna(x) else x
    )
    # Remove the original columns that are no longer needed
    df_anreiseempf.drop(columns=["Bilder", "Weiterfuehrende Links", "Text"], inplace=True)
    df_eventliste.drop(columns=["Info_Text"], inplace=True)
    df_zeitraum_info.drop(columns=["Text"], inplace=True)

    os.makedirs("data", exist_ok=True)

    path_eventliste = os.path.join("data", "eventliste_stjakob.csv")
    path_anreiseempf = os.path.join("data", "anreiseempfehlung_stjakob.csv")
    path_zeitraum_info = os.path.join("data", "zeitraum_info_stjakob.csv")
    df_eventliste.to_csv(path_eventliste, index=False)
    df_anreiseempf.to_csv(path_anreiseempf, index=False)
    df_zeitraum_info.to_csv(path_zeitraum_info, index=False)
    common.update_ftp_and_odsp(path_eventliste, "kapo/eventverkehr_st.jakob", "100419")
    common.update_ftp_and_odsp(path_anreiseempf, "kapo/eventverkehr_st.jakob", "100429")
    common.update_ftp_and_odsp(path_zeitraum_info, "kapo/eventverkehr_st.jakob", "100464")

    # Upload PNG images to FTP
    png_dir = os.path.join(DATA_ORIG_PATH, "PNG_Anfahrtsempfehlungen")
    remote_png_dir = "kapo/eventverkehr_st.jakob/png_anfahrtsempfehlungen"

    if os.path.isdir(png_dir):
        for fname in os.listdir(png_dir):
            if not fname.lower().endswith(".png"):
                continue
            local_path = os.path.join(png_dir, fname)
            try:
                common.upload_ftp(local_path, remote_path=remote_png_dir)
                logging.info("Uploaded PNG %s to FTP folder %s", fname, remote_png_dir)
            except Exception as e:
                logging.exception("Failed to upload %s: %s", local_path, e)
    else:
        logging.warning("PNG directory does not exist: %s", png_dir)


def main():
    try:
        token = get_graph_token()
        site_id = get_site_id(token)
        download_sharepoint_files(token, site_id)
    except Exception as e:
        logging.warning(
            "SharePoint download failed (%s). Falling back to existing files in '%s'.",
            e,
            DATA_ORIG_PATH,
        )
    process_data()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
