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
from decentlab import query
from dotenv import load_dotenv

load_dotenv()

# Set LOCAL_RUN=true in .env to skip SharePoint download and exceedance e-mail (offline dev on Mac).
LOCAL_RUN = os.getenv("LOCAL_RUN", "false").lower() in ("1", "true", "yes", "y")

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

# SharePoint location of the maintained exceedance workbook (read-only; we cannot write back).
EXCEEDANCE_SHAREPOINT_FOLDER = f"{SHAREPOINT_BASE}/Ueberschreitungen"
EXCEEDANCE_SHAREPOINT_PATH = f"{EXCEEDANCE_SHAREPOINT_FOLDER}/{EXCEEDANCE_LOCAL_NAME}"

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

FEINSTAUB_OUTPUT_FILE = OUTPUT_DIR / "100523_feinstaub.csv"
FEINSTAUB_METADATA_FILE = OUTPUT_DIR / "metadata_feinstaub.csv"
DUST_OUTPUT_FILE = OUTPUT_DIR / "100524_staubgebundene_schadstoffe_klybeck.csv"
VOLATILE_OUTPUT_FILE = OUTPUT_DIR / "100525_fluechtige_schadstoffe_klybeck.csv"
EXCEEDANCE_TRACKING_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck_tracking.csv"
EXCEEDANCE_OUTPUT_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck.xlsx"
PLANNED_OUTPUT_FILE = OUTPUT_DIR / "100527_geplante_messungen.xlsx"
COORDINATES_OUTPUT_FILE = OUTPUT_DIR / "100528_koordinaten_klybeck.xlsx"

DECENTLAB_DOMAIN = "bl-lufthygieneamt.decentlab.com"
DECENTLAB_API_KEY = os.getenv("API_KEY_DECENTLAB")
FEINSTAUB_DEVICES = ["16300", "16303"]

PASSIVE_PARAMS = {"Benzol", "∑CKW", "Naphthalin", "Naphtalin", "Quecksilber"}
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
    """Normalize dates to ``YYYY-MM-DD``.

    SharePoint cells may be real Excel datetimes, ISO ``YYYY-MM-DD`` strings, or
    German ``DD.MM.YYYY`` strings. ISO strings must be parsed as year-month-day:
    ``dayfirst=True`` on ``2026-07-04`` would silently turn 4 July into 7 April
    and break exceedance key matching against the local workbook.
    """
    if value is None or value == "":
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass

    if hasattr(value, "strftime") and not isinstance(value, str):
        try:
            return value.strftime("%Y-%m-%d")
        except (TypeError, ValueError, OverflowError):
            pass

    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""

    ts = pd.to_datetime(text, errors="coerce", format="%Y-%m-%d")
    if pd.isna(ts):
        ts = pd.to_datetime(text, errors="coerce", format="%d.%m.%Y")
    if pd.isna(ts):
        ts = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(ts):
        return text
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
    """Read the existing exceedance workbook downloaded from SharePoint."""
    if not EXCEEDANCE_SOURCE_FILE.exists():
        raise FileNotFoundError(f"Exceedance file not found: {EXCEEDANCE_SOURCE_FILE}")

    existing = pd.read_excel(EXCEEDANCE_SOURCE_FILE)
    for column in EXCEEDANCE_COLUMNS:
        if column not in existing.columns:
            existing[column] = ""
    return existing[EXCEEDANCE_COLUMNS]


def _merge_exceedances(new_df: pd.DataFrame, existing_df: pd.DataFrame) -> pd.DataFrame:
    """Combine freshly detected exceedances with the maintained workbook.

    The local/SharePoint workbook is the source of ``Info / Massnahmen``. New
    exceedances are added with an empty info column. Already known rows keep
    their info (never overwritten) and are not dropped just because they are
    not in the current detection set.
    """
    new_df = new_df.copy()
    existing_df = existing_df.copy()

    for frame in (new_df, existing_df):
        for date_col in ("Messbeginn", "Messende"):
            frame[date_col] = frame[date_col].apply(_format_date)
        frame["Standort"] = frame["Standort"].astype(str).str.strip()
        frame["parameter"] = frame["parameter"].astype(str).str.strip()

    existing_info: dict[tuple, str] = {}
    existing_rows: dict[tuple, pd.Series] = {}
    for _, row in existing_df.iterrows():
        key = tuple(row[col] for col in EXCEEDANCE_KEY_COLUMNS)
        info = row.get("Info / Massnahmen", "")
        info = "" if pd.isna(info) else str(info).strip()
        # Keep the first non-empty info entry if duplicate keys exist.
        if key not in existing_info or (not existing_info[key] and info):
            existing_info[key] = info
            kept = row.copy()
            kept["Info / Massnahmen"] = info
            existing_rows[key] = kept

    merged_rows = []
    seen_keys: set[tuple] = set()
    for _, row in new_df.iterrows():
        key = tuple(row[col] for col in EXCEEDANCE_KEY_COLUMNS)
        new_row = row.copy()
        new_row["Info / Massnahmen"] = existing_info.get(key, "")
        merged_rows.append(new_row)
        seen_keys.add(key)

    for key, row in existing_rows.items():
        if key not in seen_keys:
            merged_rows.append(row)

    return pd.DataFrame(merged_rows, columns=EXCEEDANCE_COLUMNS).reset_index(drop=True)


def _publish_exceedances(attachment_df: pd.DataFrame) -> None:
    """Refresh the exceedance workbook and publish it to OGD.

    The maintained workbook is merged with the freshly detected exceedances
    (keeping existing ``Info / Massnahmen``), saved to ``data_orig``, copied to
    the OGD output folder and uploaded via FTP/ODS. SharePoint is read-only for
    this job; the workbook is sent by e-mail for manual upload.
    """
    existing_df = _load_existing_exceedances()
    merged_df = _merge_exceedances(attachment_df, existing_df)

    DATA_ORIG_PATH.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    merged_df.to_excel(EXCEEDANCE_SOURCE_FILE, index=False)
    logging.info("Wrote %s exceedance rows to %s", len(merged_df), EXCEEDANCE_SOURCE_FILE)

    shutil.copyfile(EXCEEDANCE_SOURCE_FILE, EXCEEDANCE_OUTPUT_FILE)
    logging.info("Copied %s to %s", EXCEEDANCE_SOURCE_FILE, EXCEEDANCE_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(EXCEEDANCE_OUTPUT_FILE), "aue/luft/", "100526")


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

    if LOCAL_RUN:
        logging.info("LOCAL_RUN=true: skipping exceedance e-mail.")
        return

    text = "Das Klybeck Luftmessungs-ETL hat neue/veränderte Überschreitungen erkannt.\n\n"
    text += f"Warnwert-Überschreitungen (>=): {len(warn_exceedances)}\n"
    text += f"Interventionswert-Überschreitungen (>=): {len(intervention_exceedances)}\n\n"
    text += (
        f"Die aktualisierte Datei «{EXCEEDANCE_LOCAL_NAME}» ist dieser E-Mail beigefügt.\n\n"
        "Bitte ergänzen Sie die Spalte «Info / Massnahmen» und laden Sie die Datei "
        "anschliessend in den folgenden SharePoint-Ordner hoch:\n\n"
        f"{EXCEEDANCE_SHAREPOINT_FOLDER}\n"
        f"{EXCEEDANCE_SHAREPOINT_URL}\n\n"
        "Freundliche Grüsse\n"
        "Ihr automatisierter Open Data Basel-Stadt Python-Job"
    )

    msg = common.email_message(
        subject="Klybeck Luft: Überschreitungen Warnwert/Interventionswert",
        text=text,
        img=None,
        attachment=str(EXCEEDANCE_SOURCE_FILE),
    )
    common.send_email(msg)
    ct.update_hash_file(str(EXCEEDANCE_TRACKING_FILE))
    logging.info(
        "Sent exceedance e-mail with attachment %s (SharePoint folder %s)",
        EXCEEDANCE_SOURCE_FILE,
        EXCEEDANCE_SHAREPOINT_FOLDER,
    )


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
    """Download the source files from SharePoint into ``DATA_ORIG_PATH``."""
    token = get_graph_token()
    site_id = get_site_id(token)
    download_sharepoint_files(token, site_id)


def _publish_planned_measurements() -> None:
    """Copy the planned measurements workbook from data_orig and publish it.

    The file is downloaded from SharePoint into ``data_orig``. Here we copy it
    to the output folder under its OGD name and upload/publish it via FTP and ODS.
    """
    if not PLANNED_SOURCE_FILE.exists():
        raise FileNotFoundError(f"Planned measurements file not found: {PLANNED_SOURCE_FILE}")

    shutil.copyfile(PLANNED_SOURCE_FILE, PLANNED_OUTPUT_FILE)
    logging.info("Copied %s to %s", PLANNED_SOURCE_FILE, PLANNED_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(PLANNED_OUTPUT_FILE), "aue/luft/", "100527")


def _publish_coordinates() -> None:
    """Copy the measurement location coordinates workbook and publish it.

    The file is downloaded from SharePoint into ``data_orig``. Here we copy it
    to the output folder under its OGD name, upload it via FTP and trigger an ODS reload.
    """
    if not COORDINATES_SOURCE_FILE.exists():
        raise FileNotFoundError(f"Coordinates file not found: {COORDINATES_SOURCE_FILE}")

    shutil.copyfile(COORDINATES_SOURCE_FILE, COORDINATES_OUTPUT_FILE)
    logging.info("Copied %s to %s", COORDINATES_SOURCE_FILE, COORDINATES_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(COORDINATES_OUTPUT_FILE), "aue/luft/", "100528")


def _normalize_feinstaub_column_name(column_name: str, device: str) -> str:
    prefix = f"{device}."
    if column_name.startswith(prefix):
        return column_name[len(prefix) :]
    return column_name


def _transform_feinstaub_device_df(df: pd.DataFrame, device: str) -> pd.DataFrame:
    transformed = df.copy()
    transformed.columns = [_normalize_feinstaub_column_name(col, device) for col in transformed.columns]
    transformed = transformed.reset_index().rename(columns={"time": "timestamp"})
    transformed.insert(0, "standort", device)
    return transformed


def _extract_feinstaub_metadata_rows(df: pd.DataFrame, device: str) -> list[dict[str, Any]]:
    rows = []
    tags = getattr(df, "tags", {})
    for source_column, metadata in tags.items():
        rows.append(
            {
                "standort": device,
                "column": _normalize_feinstaub_column_name(source_column, device),
                "unit": metadata.get("unit"),
                "sensor": metadata.get("sensor"),
                "channel": metadata.get("channel"),
                "title": metadata.get("title"),
            }
        )
    return rows


def _publish_feinstaub() -> None:
    """Query Decentlab PM sensors, write CSVs, and publish the Feinstaub dataset."""
    if not DECENTLAB_API_KEY:
        raise RuntimeError("API_KEY_DECENTLAB is not set")

    transformed_frames: list[pd.DataFrame] = []
    metadata_rows: list[dict[str, Any]] = []

    for device in FEINSTAUB_DEVICES:
        df_device = query(
            domain=DECENTLAB_DOMAIN,
            api_key=DECENTLAB_API_KEY,
            device=f"/^{device}$/",
        )
        logging.info("Device %s: %s rows, columns: %s", device, len(df_device), list(df_device.columns))
        transformed_frames.append(_transform_feinstaub_device_df(df_device, device))
        metadata_rows.extend(_extract_feinstaub_metadata_rows(df_device, device))

    combined_df = pd.concat(transformed_frames, ignore_index=True, sort=False)
    combined_df = combined_df.sort_values(["timestamp", "standort"]).reset_index(drop=True)

    metadata_df = pd.DataFrame(metadata_rows)
    metadata_df = metadata_df.drop_duplicates().sort_values(["column", "standort"]).reset_index(drop=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    combined_df.to_csv(FEINSTAUB_OUTPUT_FILE, index=False)
    logging.info("Wrote %s rows to %s", len(combined_df), FEINSTAUB_OUTPUT_FILE)
    metadata_df.to_csv(FEINSTAUB_METADATA_FILE, index=False)
    logging.info("Wrote %s metadata rows to %s", len(metadata_df), FEINSTAUB_METADATA_FILE)
    common.update_ftp_and_odsp(str(FEINSTAUB_OUTPUT_FILE), "aue/luft/", "100523")


def main() -> None:
    """Create and publish the Klybeck air-quality datasets."""
    logging.info("ETL job started")

    if LOCAL_RUN:
        logging.info("LOCAL_RUN=true: skipping SharePoint download, using local files in %s", DATA_ORIG_PATH)
    else:
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

    expected_volatile = {
        "Benzol",
        "∑CKW",
        "Naphthalin",
        "Quecksilber",
        "∑Aniline",
        "Nitrobenzol",
        "Phenol",
        "Methylphenole",
    }
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
    _publish_feinstaub()
    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
