import logging
import os
import shutil
from pathlib import Path
from typing import Any

import common
import common.change_tracking as ct
import msal
import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_HOST = os.getenv("SHAREPOINT_HOST")
SITE_NAME = os.getenv("SHAREPOINT_SITE_NAME_AUE_LUFT_KLYBECK")
CERT_PATH = os.getenv("SHAREPOINT_CERT_PATH")
THUMBPRINT = os.getenv("SHAREPOINT_THUMBPRINT")

SHAREPOINT_ROOT = "KLYBECK"
SHAREPOINT_FOLDER = f"{SHAREPOINT_ROOT}/Immisssionsüberwachung/OGD-Daten"
DATA_ORIG_PATH = Path("data_orig")

SOURCE_FILE = DATA_ORIG_PATH / "Tabelle_KlybeckDaten_Dashboard.xlsx"
SOURCE_SHEET = "DUMMIE-D2_Abfrage-Dashboard (2)"
PLANNED_SOURCE_FILE = DATA_ORIG_PATH / "Geplante Messungen.xlsx"
OUTPUT_DIR = Path("data")

DUST_OUTPUT_FILE = OUTPUT_DIR / "100524_staubgebundene_schadstoffe_klybeck.csv"
VOLATILE_OUTPUT_FILE = OUTPUT_DIR / "100525_fluechtige_schadstoffe_klybeck.csv"
EXCEEDANCE_OUTPUT_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck.xlsx"
EXCEEDANCE_TRACKING_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck_tracking.csv"
PLANNED_OUTPUT_FILE = OUTPUT_DIR / "100527_geplante_messungen.xlsx"

PASSIVE_PARAMS = {"Benzol", "∑CKW", "Naphthalin", "Naphtalin"}
ACTIVE_PARAMS = {"∑Aniline", "Nitrobenzol", "Phenol", "Methylphenole"}
DUST_PARAMS = {"PM10", "∑PAK", "Benzo(a)pyren"}

TARGET_COLUMNS = [
    "messbeginn",
    "messende",
    "standort",
    "parameter",
    "messwert",
    "interventionswert",
    "warnwert",
    "einheit",
    "messmethode",
]

EXCEEDANCE_COLUMNS = [
    "Messbeginn",
    "Messende",
    "Standort",
    "parameter",
    "messwert_ug_m3",
    "interventionswert_ug_m3",
    "Info / Massnahmen",
]


def _normalize_parameter(value: Any) -> str:
    parameter = str(value).strip()
    if parameter == "PM 10":
        return "PM10"
    if parameter == "Naphtalin":
        return "Naphthalin"
    return parameter


def _format_date(value: Any) -> str:
    if pd.isna(value):
        return ""
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value).strip()
    return ts.strftime("%Y-%m-%d")


def _format_number(value: Any, decimals: int | None = None) -> str:
    if pd.isna(value) or value == "":
        return ""
    number = float(value)
    if decimals is None:
        return f"{number:g}"
    return f"{number:.{decimals}f}"


def _to_float(value: Any) -> float | None:
    if pd.isna(value) or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _messmethode(parameter: str) -> str:
    if parameter in PASSIVE_PARAMS:
        return "VOC-Passivsammler"
    if parameter in ACTIVE_PARAMS:
        return "Aktivsammler"
    if parameter in DUST_PARAMS:
        return "Gravimetrie"
    return ""


def _build_exceedance_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    numeric = df.copy()
    numeric["messwert_num"] = numeric["messwert"].apply(_to_float)
    numeric["warnwert_num"] = numeric["warnwert"].apply(_to_float)
    numeric["interventionswert_num"] = numeric["interventionswert"].apply(_to_float)

    warn_exceedances = numeric[
        numeric["messwert_num"].notna()
        & numeric["warnwert_num"].notna()
        & (numeric["messwert_num"] >= numeric["warnwert_num"])
    ].copy()
    intervention_exceedances = numeric[
        numeric["messwert_num"].notna()
        & numeric["interventionswert_num"].notna()
        & (numeric["messwert_num"] >= numeric["interventionswert_num"])
    ].copy()

    return warn_exceedances, intervention_exceedances


def _build_excel_attachment(intervention_exceedances: pd.DataFrame) -> pd.DataFrame:
    attachment = intervention_exceedances[
        ["messbeginn", "messende", "standort", "parameter", "messwert", "interventionswert"]
    ].copy()
    attachment = attachment.rename(
        columns={
            "messbeginn": "Messbeginn",
            "messende": "Messende",
            "standort": "Standort",
            "messwert": "messwert_ug_m3",
            "interventionswert": "interventionswert_ug_m3",
        }
    )
    attachment["Info / Massnahmen"] = ""
    return attachment.reindex(columns=EXCEEDANCE_COLUMNS)


def _send_exceedance_email_if_changed(
    attachment_df: pd.DataFrame,
    warn_exceedances: pd.DataFrame,
    intervention_exceedances: pd.DataFrame,
) -> None:
    tracking_df = attachment_df.fillna("").sort_values(
        [
            "Messbeginn",
            "Messende",
            "Standort",
            "parameter",
            "messwert_ug_m3",
            "interventionswert_ug_m3",
        ]
    )
    tracking_df.to_csv(EXCEEDANCE_TRACKING_FILE, sep=";", index=False, encoding="utf-8")

    if not ct.has_changed(str(EXCEEDANCE_TRACKING_FILE)):
        logging.info("No change in exceedance content. Skipping workbook update and e-mail.")
        return

    attachment_df.to_excel(EXCEEDANCE_OUTPUT_FILE, index=False)
    text = "Das Klybeck Luftmessungs-ETL hat neue/veraenderte Ueberschreitungen erkannt.\n\n"
    text += f"Warnwert-Ueberschreitungen (>=): {len(warn_exceedances)}\n"
    text += f"Interventionswert-Ueberschreitungen (>=): {len(intervention_exceedances)}\n\n"
    text += "Im Anhang finden Sie die Datei mit Interventionswert-Ueberschreitungen.\n"
    text += "Spalte 'Info / Massnahmen' ist fuer manuelle Ergaenzungen vorgesehen.\n\n"
    text += "Kind regards,\nYour automated Open Data Basel-Stadt Python Job"

    msg = common.email_message(
        subject="Klybeck Luft: Ueberschreitungen Warnwert/Interventionswert",
        text=text,
        img=None,
        attachment=str(EXCEEDANCE_OUTPUT_FILE),
    )
    common.send_email(msg)
    ct.update_hash_file(str(EXCEEDANCE_TRACKING_FILE))
    logging.info("Sent exceedance e-mail with attachment %s", EXCEEDANCE_OUTPUT_FILE)


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
    local_dir: Path,
) -> None:
    """Download all files from a SharePoint folder recursively."""
    headers = {"Authorization": f"Bearer {token}"}

    local_dir.mkdir(parents=True, exist_ok=True)

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{sharepoint_folder}:/children"

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    items = r.json().get("value", [])

    for item in items:
        name = item["name"]

        if "folder" in item:
            sub_sp_path = f"{sharepoint_folder}/{name}"
            sub_local_dir = local_dir / name

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
        dest_path = local_dir / name

        logging.info("Downloading %s/%s", sharepoint_folder, name)

        file_r = requests.get(download_url, stream=True)
        file_r.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in file_r.iter_content(chunk_size=8192):
                f.write(chunk)


def download_sharepoint_files(token: str, site_id: str) -> None:
    drive_id = get_drive_id(token, site_id)

    download_folder(
        token=token,
        drive_id=drive_id,
        sharepoint_folder=SHAREPOINT_FOLDER,
        local_dir=DATA_ORIG_PATH,
    )


def _to_long_schema(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, str]] = []

    # First 4 rows contain metadata, following rows are measurement periods.
    for col_idx, col_name in enumerate(df.columns):
        if col_idx < 3:
            continue

        parameter = _normalize_parameter(df.iloc[0, col_idx])
        standort = str(col_name).split(".")[0].strip()
        interventionswert = _format_number(df.iloc[1, col_idx])
        warnwert = _format_number(df.iloc[2, col_idx])
        einheit = "" if pd.isna(df.iloc[3, col_idx]) else str(df.iloc[3, col_idx]).strip()
        messmethode = _messmethode(parameter)

        for row_idx in range(4, len(df)):
            messbeginn = _format_date(df.iloc[row_idx, 1])
            messende = _format_date(df.iloc[row_idx, 2])
            messwert = _format_number(df.iloc[row_idx, col_idx])

            record = {column: "" for column in TARGET_COLUMNS}
            record.update(
                {
                    "messbeginn": messbeginn,
                    "messende": messende,
                    "standort": standort,
                    "parameter": parameter,
                    "messwert": messwert,
                    "interventionswert": interventionswert,
                    "warnwert": warnwert,
                    "einheit": einheit,
                    "messmethode": messmethode,
                }
            )
            records.append(record)

    normalized = pd.DataFrame(records, columns=TARGET_COLUMNS)
    return normalized[
        ~(normalized["messbeginn"].eq("") & normalized["messende"].eq("") & normalized["messwert"].eq(""))
    ].reset_index(drop=True)


def fetch_source_file() -> None:
    """Download the source file from SharePoint, falling back to the local copy.

    SharePoint writes into ``DATA_ORIG_PATH`` (the same folder we read the source
    file from). If the download fails for any reason, we keep using the file that
    is already present in ``data_orig``.
    """
    try:
        token = get_graph_token()
        site_id = get_site_id(token)
        download_sharepoint_files(token, site_id)
    except Exception:
        logging.exception("SharePoint download failed. Falling back to local file in %s", DATA_ORIG_PATH)
        if not SOURCE_FILE.exists():
            raise FileNotFoundError(
                f"SharePoint download failed and no local fallback file found: {SOURCE_FILE}"
            )
        logging.warning("Using existing local source file %s", SOURCE_FILE)


def _publish_planned_measurements() -> None:
    """Copy the planned measurements workbook from data_orig and publish it.

    The file is downloaded from SharePoint into ``data_orig`` (with the local
    fallback handled in ``fetch_source_file``). Here we simply copy it to the
    output folder under its OGD name and upload/publish it via FTP and ODS.
    """
    if not PLANNED_SOURCE_FILE.exists():
        raise FileNotFoundError(f"Planned measurements file not found: {PLANNED_SOURCE_FILE}")

    shutil.copyfile(PLANNED_SOURCE_FILE, PLANNED_OUTPUT_FILE)
    logging.info("Copied %s to %s", PLANNED_SOURCE_FILE, PLANNED_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(PLANNED_OUTPUT_FILE), "aue/luft/", "100527")


def main() -> None:
    """Create two Klybeck pollutant CSV files with the target schema."""
    logging.info("ETL job started")

    fetch_source_file()

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_df = pd.read_excel(SOURCE_FILE, sheet_name=SOURCE_SHEET)
    long_df = _to_long_schema(source_df)
    warn_exceedances, intervention_exceedances = _build_exceedance_df(long_df)
    attachment_df = _build_excel_attachment(intervention_exceedances)

    volatile_params = PASSIVE_PARAMS.union(ACTIVE_PARAMS)
    volatile_df = long_df[long_df["parameter"].isin(volatile_params)].copy()
    dust_df = long_df[long_df["parameter"].isin(DUST_PARAMS)].copy()

    expected_volatile = {"Benzol", "∑CKW", "Naphthalin", "∑Aniline", "Nitrobenzol", "Phenol", "Methylphenole"}
    expected_dust = DUST_PARAMS

    missing_volatile = expected_volatile - set(volatile_df["parameter"].unique())
    missing_dust = expected_dust - set(dust_df["parameter"].unique())
    if missing_volatile:
        raise ValueError(f"Missing volatile parameters: {sorted(missing_volatile)}")
    if missing_dust:
        raise ValueError(f"Missing dust parameters: {sorted(missing_dust)}")
    if volatile_df.empty or dust_df.empty:
        raise ValueError("One or both output datasets are empty.")

    volatile_df.to_csv(VOLATILE_OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    logging.info("Wrote %s rows to %s", len(volatile_df), VOLATILE_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(VOLATILE_OUTPUT_FILE), "aue/luft/", "100525")
    dust_df.to_csv(DUST_OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    logging.info("Wrote %s rows to %s", len(dust_df), DUST_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(DUST_OUTPUT_FILE), "aue/luft/", "100524")
    _send_exceedance_email_if_changed(attachment_df, warn_exceedances, intervention_exceedances)
    _publish_planned_measurements()
    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
