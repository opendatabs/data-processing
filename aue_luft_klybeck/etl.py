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

SHAREPOINT_ROOT = "Klybeck"
SHAREPOINT_BASE = f"{SHAREPOINT_ROOT}/Immissionsüberwachung/Dashboard_Klybeck"
DATA_ORIG_PATH = Path("data_orig")

SOURCE_LOCAL_NAME = "Tabelle_KlybeckDaten_Dashboard.xlsx"
PLANNED_LOCAL_NAME = "Geplante_Messungen.xlsx"
COORDINATES_LOCAL_NAME = "Koordinaten_Messstandorte_Klybeck.xlsx"
EXCEEDANCE_LOCAL_NAME = "Gemessene_Ueberschreitungen.xlsx"

# SharePoint location of the maintained exceedance workbook we read from and write back to.
EXCEEDANCE_SHAREPOINT_PATH = f"{SHAREPOINT_BASE}/Ueberschreitungen/Gemessene_Ueberschreitungen.xlsx"

# Map of SharePoint file paths to the local file names we store them under.
SHAREPOINT_FILES = {
    f"{SHAREPOINT_BASE}/Entwicklung Tabelle/ENTWURF_Auswertungstabelle_Klybeck_NEU.xlsx": SOURCE_LOCAL_NAME,
    f"{SHAREPOINT_BASE}/Planung Messungen/Geplante_Messungen.xlsx": PLANNED_LOCAL_NAME,
    f"{SHAREPOINT_BASE}/Planung Messungen/Koordinaten_Messstandorte_Klybeck.xlsx": COORDINATES_LOCAL_NAME,
    EXCEEDANCE_SHAREPOINT_PATH: EXCEEDANCE_LOCAL_NAME,
}

# Public SharePoint location of the maintained exceedance workbook (linked in e-mails).
EXCEEDANCE_SHAREPOINT_URL = (
    "https://baselstadt.sharepoint.com/sites/ArG-Transformations-Areale/"
    "Freigegebene%20Dokumente/Forms/AllItems.aspx?id=%2Fsites%2FArG-Transformations-Areale"
    "%2FFreigegebene%20Dokumente%2FKlybeck%2FImmissions%C3%BCberwachung"
    "%2FDashboard_Klybeck%2FUeberschreitungen"
)

SOURCE_FILE = DATA_ORIG_PATH / SOURCE_LOCAL_NAME
SOURCE_SHEET = "DUMMIE-D2_Abfrage-Dashboard (2)"
PLANNED_SOURCE_FILE = DATA_ORIG_PATH / PLANNED_LOCAL_NAME
COORDINATES_SOURCE_FILE = DATA_ORIG_PATH / COORDINATES_LOCAL_NAME
EXCEEDANCE_SOURCE_FILE = DATA_ORIG_PATH / EXCEEDANCE_LOCAL_NAME
OUTPUT_DIR = Path("data")

DUST_OUTPUT_FILE = OUTPUT_DIR / "100524_staubgebundene_schadstoffe_klybeck.csv"
VOLATILE_OUTPUT_FILE = OUTPUT_DIR / "100525_fluechtige_schadstoffe_klybeck.csv"
EXCEEDANCE_TRACKING_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck_tracking.csv"
PLANNED_OUTPUT_FILE = OUTPUT_DIR / "100527_geplante_messungen.xlsx"
COORDINATES_OUTPUT_FILE = OUTPUT_DIR / "100528_koordinaten_klybeck.xlsx"

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

# Natural key identifying a single exceedance event (one measurement period at one
# location for one parameter). Used to carry over manually maintained "Info /
# Massnahmen" entries when refreshing the exceedance workbook.
EXCEEDANCE_KEY_COLUMNS = ["Messbeginn", "Messende", "Standort", "parameter"]


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


def _load_existing_exceedances() -> pd.DataFrame:
    """Read the existing exceedance workbook from ``data_orig``.

    SharePoint writes the maintained workbook into ``data_orig`` (with the local
    fallback handled in ``fetch_source_file``), so here we only read the local
    copy. If no file is present yet, we start with an empty frame.
    """
    if not EXCEEDANCE_SOURCE_FILE.exists():
        logging.warning(
            "No existing exceedance file found at %s. Starting with an empty workbook.",
            EXCEEDANCE_SOURCE_FILE,
        )
        return pd.DataFrame(columns=EXCEEDANCE_COLUMNS)

    existing = pd.read_excel(EXCEEDANCE_SOURCE_FILE)
    for column in EXCEEDANCE_COLUMNS:
        if column not in existing.columns:
            existing[column] = ""
    return existing[EXCEEDANCE_COLUMNS]


def _merge_exceedances(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """Combine freshly detected exceedances with the maintained workbook.

    New exceedances are added with an empty ``Info / Massnahmen`` column, while
    manually maintained ``Info / Massnahmen`` entries for already known
    exceedances are carried over and never overwritten.
    """
    new_df = new_df.copy()
    existing_df = existing_df.copy()

    for frame in (new_df, existing_df):
        for date_col in ("Messbeginn", "Messende"):
            frame[date_col] = frame[date_col].apply(_format_date)
        frame["Standort"] = frame["Standort"].astype(str).str.strip()
        frame["parameter"] = frame["parameter"].astype(str).str.strip()

    existing_info: dict[tuple, str] = {}
    for _, row in existing_df.iterrows():
        key = tuple(row[col] for col in EXCEEDANCE_KEY_COLUMNS)
        info = row.get("Info / Massnahmen", "")
        info = "" if pd.isna(info) else str(info).strip()
        # Keep the first non-empty info entry if duplicate keys exist.
        if key not in existing_info or (not existing_info[key] and info):
            existing_info[key] = info

    merged_rows = []
    for _, row in new_df.iterrows():
        key = tuple(row[col] for col in EXCEEDANCE_KEY_COLUMNS)
        new_row = row.copy()
        new_row["Info / Massnahmen"] = existing_info.get(key, "")
        merged_rows.append(new_row)

    return pd.DataFrame(merged_rows, columns=EXCEEDANCE_COLUMNS).reset_index(drop=True)


def _publish_exceedances(attachment_df: pd.DataFrame) -> None:
    """Refresh the exceedance workbook and write it back to SharePoint.

    The maintained workbook is merged with the freshly detected exceedances
    (keeping existing ``Info / Massnahmen``), saved to ``data_orig`` and then
    uploaded to SharePoint. If the upload fails, the local copy in ``data_orig``
    is kept as the fallback.
    """
    existing_df = _load_existing_exceedances()
    merged_df = _merge_exceedances(attachment_df, existing_df)

    DATA_ORIG_PATH.mkdir(parents=True, exist_ok=True)
    merged_df.to_excel(EXCEEDANCE_SOURCE_FILE, index=False)
    logging.info("Wrote %s exceedance rows to %s", len(merged_df), EXCEEDANCE_SOURCE_FILE)

    try:
        token = get_graph_token()
        site_id = get_site_id(token)
        drive_id = get_drive_id(token, site_id)
        upload_file(token, drive_id, EXCEEDANCE_SHAREPOINT_PATH, EXCEEDANCE_SOURCE_FILE)
        logging.info("Uploaded exceedance workbook to SharePoint: %s", EXCEEDANCE_SHAREPOINT_PATH)
    except Exception:
        logging.exception(
            "SharePoint upload of exceedance workbook failed. Kept local copy in %s",
            EXCEEDANCE_SOURCE_FILE,
        )


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
        logging.info("No change in exceedance content. Skipping e-mail.")
        return

    text = "Das Klybeck Luftmessungs-ETL hat neue/veraenderte Ueberschreitungen erkannt.\n\n"
    text += f"Warnwert-Ueberschreitungen (>=): {len(warn_exceedances)}\n"
    text += f"Interventionswert-Ueberschreitungen (>=): {len(intervention_exceedances)}\n\n"
    text += "Die Datei mit den gemessenen Ueberschreitungen finden Sie auf SharePoint:\n"
    text += f"{EXCEEDANCE_SHAREPOINT_URL}\n\n"
    text += "Spalte 'Info / Massnahmen' ist fuer manuelle Ergaenzungen vorgesehen.\n\n"
    text += "Kind regards,\nYour automated Open Data Basel-Stadt Python Job"

    msg = common.email_message(
        subject="Klybeck Luft: Ueberschreitungen Warnwert/Interventionswert",
        text=text,
        img=None,
        attachment=None,
    )
    common.send_email(msg)
    ct.update_hash_file(str(EXCEEDANCE_TRACKING_FILE))
    logging.info("Sent exceedance e-mail with SharePoint link %s", EXCEEDANCE_SHAREPOINT_URL)


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


def upload_file(
    token: str,
    drive_id: str,
    sharepoint_path: str,
    src_path: Path,
) -> None:
    """Upload a single local file to ``sharepoint_path`` on SharePoint."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream",
    }

    url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{sharepoint_path}:/content"

    logging.info("Uploading %s to %s", src_path, sharepoint_path)

    with open(src_path, "rb") as f:
        r = requests.put(url, headers=headers, data=f)
    r.raise_for_status()


def download_sharepoint_files(token: str, site_id: str) -> None:
    drive_id = get_drive_id(token, site_id)

    DATA_ORIG_PATH.mkdir(parents=True, exist_ok=True)

    for sharepoint_path, local_name in SHAREPOINT_FILES.items():
        download_file(
            token=token,
            drive_id=drive_id,
            sharepoint_path=sharepoint_path,
            dest_path=DATA_ORIG_PATH / local_name,
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


def _publish_coordinates() -> None:
    """Copy the measurement location coordinates workbook and publish it.

    The file is downloaded from SharePoint into ``data_orig`` (with the local
    fallback handled in ``fetch_source_file``). Here we copy it to the output
    folder under its OGD name, upload it via FTP and trigger an ODS reload.
    """
    if not COORDINATES_SOURCE_FILE.exists():
        raise FileNotFoundError(f"Coordinates file not found: {COORDINATES_SOURCE_FILE}")

    shutil.copyfile(COORDINATES_SOURCE_FILE, COORDINATES_OUTPUT_FILE)
    logging.info("Copied %s to %s", COORDINATES_SOURCE_FILE, COORDINATES_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(COORDINATES_OUTPUT_FILE), "aue/luft/", "100528")


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
    _publish_exceedances(attachment_df)
    _send_exceedance_email_if_changed(attachment_df, warn_exceedances, intervention_exceedances)
    _publish_planned_measurements()
    _publish_coordinates()
    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
