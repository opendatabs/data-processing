import json
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

LOGGER = logging.getLogger(__name__)

TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_HOST = os.getenv("SHAREPOINT_HOST")
SITE_NAME = os.getenv("SHAREPOINT_SITE_NAME_STAKA_GUTACHTEN")
CERT_PATH = os.getenv("SHAREPOINT_CERT_PATH")
THUMBPRINT = os.getenv("SHAREPOINT_THUMBPRINT")

SHAREPOINT_ROOT = "General"

DATA_ORIG_PATH = "data_orig"
NOTIFIED_MISMATCHES_JSON = Path("change_tracking") / "gutachten_notified_mismatches.json"

DEPARTEMENTS = ["BVD", "ED", "FD", "GD", "JSD", "PD", "WSU", "Staatskanzlei"]


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


def list_drive_children(token: str, drive_id: str, sharepoint_folder: str) -> list[dict]:
    """List every child of a drive folder, following Graph pagination."""
    headers = {"Authorization": f"Bearer {token}"}
    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{sharepoint_folder}:/children"
    items: list[dict] = []

    while url:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        payload = r.json()
        items.extend(payload.get("value", []))
        url = payload.get("@odata.nextLink")

    return items


def download_folder(
    token: str,
    drive_id: str,
    sharepoint_folder: str,
    local_dir: str,
    *,
    flatten: bool = False,
    departement: str | None = None,
    file_departments: dict[str, str] | None = None,
):
    """
    Download all files from a SharePoint folder recursively.

    With flatten=True, files from subfolders are written into local_dir
    instead of a mirrored subdirectory, so later steps can see them.
    """

    os.makedirs(local_dir, exist_ok=True)

    for item in list_drive_children(token, drive_id, sharepoint_folder):
        name = item["name"]

        # Folder
        if "folder" in item:
            sub_sp_path = f"{sharepoint_folder}/{name}"
            sub_local_dir = local_dir if flatten else os.path.join(local_dir, name)

            download_folder(
                token,
                drive_id,
                sub_sp_path,
                sub_local_dir,
                flatten=flatten,
                departement=departement,
                file_departments=file_departments,
            )

            continue

        # File
        if "file" not in item:
            continue

        if file_departments is not None and name in file_departments:
            logging.warning(
                "Skipping duplicate %s/%s; keeping the copy already downloaded from departement %s.",
                sharepoint_folder,
                name,
                file_departments[name],
            )
            continue

        download_url = item["@microsoft.graph.downloadUrl"]

        dest_path = os.path.join(local_dir, name)

        logging.info(f"Downloading {sharepoint_folder}/{name}")

        file_r = requests.get(download_url, stream=True)
        file_r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in file_r.iter_content(chunk_size=8192):
                f.write(chunk)

        if file_departments is not None and departement is not None:
            file_departments[name] = departement


def download_sharepoint_files(token: str, site_id: str) -> dict[str, str]:
    """
    Download:
    - Excel-Datei/Liste_Gutachten.xlsx
    - Gutachten/<Departement>/**  (recursive, flattened into data_orig)

    Returns a map of downloaded Gutachten filename -> departement folder name.
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
    # Download Gutachten PDFs, including files in subfolders
    # ------------------------------------------------------------------

    file_departments: dict[str, str] = {}

    for departement in DEPARTEMENTS:
        download_folder(
            token=token,
            drive_id=drive_id,
            sharepoint_folder=(f"{SHAREPOINT_ROOT}/Gutachten/{departement}"),
            local_dir=DATA_ORIG_PATH,
            flatten=True,
            departement=departement,
            file_departments=file_departments,
        )

    return file_departments


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

    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"

    return "".join(c for c in name if c in allowed)


def load_notified_mismatches(path: Path | str = NOTIFIED_MISMATCHES_JSON) -> dict[str, set[str]]:
    """Return mismatch filenames that were already included in a successfully sent notification."""
    path = Path(path)
    if not path.exists():
        return {"unlisted": set(), "missing": set()}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        LOGGER.warning("Existing %s could not be read (%s); treating as empty.", path, exc)
        return {"unlisted": set(), "missing": set()}
    if not isinstance(payload, dict):
        return {"unlisted": set(), "missing": set()}
    return {
        "unlisted": {str(item) for item in payload.get("unlisted", []) if item is not None},
        "missing": {str(item) for item in payload.get("missing", []) if item is not None},
    }


def save_notified_mismatches(
    unlisted: set[str],
    missing: set[str],
    path: Path | str = NOTIFIED_MISMATCHES_JSON,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "unlisted": sorted(unlisted),
        "missing": sorted(missing),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def build_mismatch_email_text(
    unlisted: set[str],
    missing: set[str],
    file_departments: dict[str, str] | None = None,
) -> str:
    """Human-readable German notification body for Excel/PDF mismatches."""
    text = (
        "Beim ETL-Lauf für die Gutachten (Dataset 100489) wurden Abweichungen zwischen "
        "den PDF-Dateien und der Excel-Liste (Liste_Gutachten.xlsx) festgestellt.\n\n"
        "Der Job läuft weiter und schreibt die Daten trotzdem:\n"
        " - Nur Datei (ohne Excel-Eintrag): Zeile mit URL_Datei und Departement aus dem SharePoint-Ordner\n"
        " - Nur Metadaten (ohne Datei): Zeile mit Metadaten, ohne URL_Datei\n"
    )

    if unlisted:
        text += f"\nDateien vorhanden, aber nicht in Liste_Gutachten – {len(unlisted)} Stück:\n"
        for name in sorted(unlisted):
            departement = (file_departments or {}).get(name)
            if departement:
                text += f" - {name} ({departement})\n"
            else:
                text += f" - {name}\n"

    if missing:
        text += f"\nIn Liste_Gutachten eingetragen, aber Datei fehlt – {len(missing)} Stück:\n"
        for name in sorted(missing):
            text += f" - {name}\n"

    text += (
        "\nBitte prüfen Sie die Dateien auf SharePoint "
        f"({SHAREPOINT_ROOT}/Excel-Datei bzw. {SHAREPOINT_ROOT}/Gutachten).\n"
    )
    text += "\nFreundliche Grüsse, \nEuer automatisierter Open Data Basel-Stadt Python Job"
    return text


def notify_file_mismatches(
    unlisted_files: set[str],
    missing_files: set[str],
    *,
    file_departments: dict[str, str] | None = None,
    state_path: Path | str = NOTIFIED_MISMATCHES_JSON,
) -> None:
    """
    Send a notification e-mail only when there is new mismatch information.

    Persistent mismatches would otherwise trigger the same mail on every run.
    Never fails the ETL.
    """
    notified = load_notified_mismatches(state_path)
    # If a previously reported mismatch is resolved, forget it so a later recurrence is mailed again.
    notified["unlisted"] &= unlisted_files
    notified["missing"] &= missing_files

    new_unlisted = unlisted_files - notified["unlisted"]
    new_missing = missing_files - notified["missing"]

    if not new_unlisted and not new_missing:
        try:
            save_notified_mismatches(notified["unlisted"], notified["missing"], state_path)
        except OSError as exc:
            LOGGER.warning("Could not persist Gutachten notification state: %s", exc)
        if unlisted_files or missing_files:
            LOGGER.info(
                "Gutachten still has %s unlisted and %s missing file(s), all previously notified; no e-mail sent.",
                len(unlisted_files),
                len(missing_files),
            )
        else:
            LOGGER.info("All files in 'data_orig' are listed in 'Liste_Gutachten' and vice versa.")
        return

    LOGGER.info(
        "Detected %s new unlisted and %s new missing Gutachten file(s) "
        "(%s unlisted / %s missing already notified, skipped); sending notification e-mail.",
        len(new_unlisted),
        len(new_missing),
        len(unlisted_files) - len(new_unlisted),
        len(missing_files) - len(new_missing),
    )
    text = build_mismatch_email_text(new_unlisted, new_missing, file_departments)
    try:
        msg = common.email_message(
            subject="Gutachten (100489): Abweichungen zwischen PDFs und Liste_Gutachten.",
            text=text,
            img=None,
            attachment=None,
        )
        common.send_email(msg)
        notified["unlisted"] |= new_unlisted
        notified["missing"] |= new_missing
        save_notified_mismatches(notified["unlisted"], notified["missing"], state_path)
        LOGGER.info("Mismatch-notification e-mail sent.")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        LOGGER.warning("Could not send Gutachten mismatch-notification e-mail: %s", exc)
        try:
            save_notified_mismatches(notified["unlisted"], notified["missing"], state_path)
        except OSError as persist_exc:
            LOGGER.warning("Could not persist Gutachten notification state: %s", persist_exc)


def departement_column_name(columns) -> str | None:
    """Return the existing Departement column, matching the Excel header regardless of case."""
    for col in columns:
        if str(col).strip().lower() == "departement":
            return str(col)
    return None


def process_excel_file(file_departments: dict[str, str] | None = None):
    excel_filename = "Liste_Gutachten.xlsx"

    excel_file_path = os.path.join(
        DATA_ORIG_PATH,
        excel_filename,
    )

    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"The file '{excel_filename}' does not exist in '{DATA_ORIG_PATH}'.")

    df = pd.read_excel(excel_file_path)

    df["Dateiname"] = df["Dateiname"].astype(str).str.strip()

    def ensure_pdf_name(name: str) -> str:
        return name if Path(name).suffix else f"{name}.pdf"

    df["Dateiname"] = df["Dateiname"].apply(ensure_pdf_name)

    df["Dateiname_ftp"] = df["Dateiname"].apply(sanitize_filename)

    def ensure_pdf_suffix(
        orig_name: str,
        ftp_name: str,
    ) -> str:
        if Path(orig_name).suffix.lower() == ".pdf" and Path(ftp_name).suffix.lower() != ".pdf":
            return str(Path(ftp_name).with_suffix(".pdf"))

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

    files_in_data_orig = {f for f in os.listdir(DATA_ORIG_PATH) if os.path.isfile(os.path.join(DATA_ORIG_PATH, f))}

    listed_files = set(df["Dateiname"])

    ignored = {
        ".gitkeep",
        "Liste_Gutachten.xlsx",
        "DESKTOP.INI",
    }

    unlisted_files = files_in_data_orig - listed_files - ignored
    missing_files = listed_files - files_in_data_orig

    if file_departments is None:
        file_departments = {}

    notify_file_mismatches(unlisted_files, missing_files, file_departments=file_departments)

    # Metadata only (Excel row, no PDF): keep metadata, leave URL empty.
    df["URL_Datei"] = [
        (gate_url + ftp_name) if orig_name not in missing_files else pd.NA
        for orig_name, ftp_name in zip(df["Dateiname"], df["Dateiname_ftp"])
    ]

    # Document only (PDF without Excel row): add a row with Dateiname, URL_Datei
    # and the departement taken from the SharePoint folder the file was downloaded from.
    if unlisted_files:
        LOGGER.warning(
            "Adding %s unlisted file(s) to the dataset with URL and Departement (no other Excel metadata).",
            len(unlisted_files),
        )
        departement_col = departement_column_name(df.columns)
        if departement_col is None:
            departement_col = "Departement"
            df[departement_col] = pd.NA
        extra_rows = []
        for orig_name in sorted(unlisted_files):
            ftp_name = ensure_pdf_suffix(orig_name, sanitize_filename(orig_name))
            row = {col: pd.NA for col in df.columns}
            row["Dateiname"] = orig_name
            row["Dateiname_ftp"] = ftp_name
            row["URL_Datei"] = gate_url + ftp_name
            row[departement_col] = file_departments.get(orig_name, pd.NA)
            extra_rows.append(row)
        df = pd.concat([df, pd.DataFrame(extra_rows)], ignore_index=True)

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

        if not os.path.isfile(src_path):
            logging.info(f"Skipping upload for missing file {orig_name}")
            continue

        dst_path = os.path.join(
            "data",
            ftp_name,
        )

        shutil.copy2(src_path, dst_path)

        common.upload_ftp(
            dst_path,
            remote_path=remote_dir,
        )

        logging.info(f"Uploaded {orig_name} as {ftp_name}")

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

    file_departments = download_sharepoint_files(token, site_id)

    df = process_excel_file(file_departments)

    upload_files_to_ftp(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    main()

    logging.info("Job successful.")
