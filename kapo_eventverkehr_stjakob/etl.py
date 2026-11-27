import datetime
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

COLUMNS_OF_INTEREST_EVENTLISTE = [
    "id",
    "name",
    "link",
    "standort",
    "datum",
    "beginn",
    "ende",
    "sperrung",
    "zusatzpp",
    "ticketintegration",
    "bemerkung",
    "info_text_html",
]

FTP_FOLDER = "kapo/eventverkehr_st.jakob"
ICS_FTP_FOLDER = f"{FTP_FOLDER}/calendar"
ICS_URL_BASE = f"https://data-bs.ch/stata/{ICS_FTP_FOLDER}/"
ICS_LOCAL_DIR = os.path.join("data", "calendar")
ICS_TZID = "Europe/Zurich"
ICS_VTIMEZONE = [
    "BEGIN:VTIMEZONE",
    f"TZID:{ICS_TZID}",
    "BEGIN:DAYLIGHT",
    "TZOFFSETFROM:+0100",
    "TZOFFSETTO:+0200",
    "TZNAME:CEST",
    "DTSTART:19700329T020000",
    "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU",
    "END:DAYLIGHT",
    "BEGIN:STANDARD",
    "TZOFFSETFROM:+0200",
    "TZOFFSETTO:+0100",
    "TZNAME:CET",
    "DTSTART:19701025T030000",
    "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU",
    "END:STANDARD",
    "END:VTIMEZONE",
]


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
    print("Acess Token: ", result["access_token"])
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


def ics_escape(value) -> str:
    text = str(value).strip()
    return (
        text.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\r\n", "\\n").replace("\n", "\\n")
    )


def ics_fold(line: str) -> str:
    """Fold content lines to max. 75 octets as required by RFC 5545."""
    encoded = line.encode("utf-8")
    if len(encoded) <= 75:
        return line
    parts, current = [], b""
    limit = 75
    for char in line:
        char_bytes = char.encode("utf-8")
        if len(current) + len(char_bytes) > limit:
            parts.append(current.decode("utf-8"))
            current = b""
            limit = 74
        current += char_bytes
    parts.append(current.decode("utf-8"))
    return "\r\n ".join(parts)


def parse_time(value) -> datetime.time | None:
    if value is None or (not isinstance(value, datetime.time) and pd.isna(value)):
        return None
    if isinstance(value, datetime.datetime):
        return value.time()
    if isinstance(value, datetime.time):
        return value
    match = re.match(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", str(value))
    if not match:
        logging.warning("Could not parse time %r", value)
        return None
    return datetime.time(int(match.group(1)), int(match.group(2)), int(match.group(3) or 0))


def format_time(value):
    parsed = parse_time(value)
    return parsed.strftime("%H:%M") if parsed is not None else value


def is_filled(value) -> bool:
    return value is not None and not pd.isna(value) and str(value).strip() != ""


def build_ics(event: dict) -> str:
    """Build an iCalendar file for a single event (all-day if no Beginn is given)."""
    event_id = event["id"]
    datum = pd.to_datetime(event["datum"]).date()
    beginn = parse_time(event["beginn"])
    ende = parse_time(event["ende"])

    if beginn is None:
        timing = [
            f"DTSTART;VALUE=DATE:{datum:%Y%m%d}",
            f"DTEND;VALUE=DATE:{datum + datetime.timedelta(days=1):%Y%m%d}",
        ]
    else:
        start = datetime.datetime.combine(datum, beginn)
        timing = [f"DTSTART;TZID={ICS_TZID}:{start:%Y%m%dT%H%M%S}"]
        if ende is not None:
            end = datetime.datetime.combine(datum, ende)
            if end <= start:
                end += datetime.timedelta(days=1)
            timing.append(f"DTEND;TZID={ICS_TZID}:{end:%Y%m%dT%H%M%S}")

    description = []
    if is_filled(event["bemerkung"]):
        description.append(str(event["bemerkung"]).strip())
    if str(event["sperrung"]).strip().lower() == "ja":
        description.append(
            "Voraussichtlich wird die Kreuzung im Raum St. Jakob für den motorisierten "
            "Individualverkehr zeitweise gesperrt."
        )
    if event["ticketintegration"] is True or str(event["ticketintegration"]).strip().lower() in ("true", "1", "ja"):
        description.append(
            "Das ÖV-Ticket für die Hin- und Rückreise innerhalb des TNW-Gebiets ist im Veranstaltungspreis inkludiert."
        )
    if is_filled(event["link"]):
        description.append(str(event["link"]).strip())

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Kanton Basel-Stadt//Veranstaltungen im Raum St. Jakob//DE",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        *ICS_VTIMEZONE,
        "BEGIN:VEVENT",
        f"UID:stjakob-event-{event_id}@data.bs.ch",
        f"DTSTAMP:{datum:%Y%m%d}T000000Z",
        *timing,
        f"SUMMARY:{ics_escape(event['name'])}",
    ]
    if is_filled(event["standort"]):
        lines.append(f"LOCATION:{ics_escape(event['standort'])}")
    if description:
        lines.append(f"DESCRIPTION:{ics_escape(chr(10).join(description))}")
    if is_filled(event["link"]):
        lines.append(f"URL:{str(event['link']).strip()}")
    lines += ["TRANSP:TRANSPARENT", "END:VEVENT", "END:VCALENDAR"]
    return "\r\n".join(ics_fold(line) for line in lines) + "\r\n"


def create_ics_files(df_eventliste: pd.DataFrame) -> pd.Series:
    """Write one ICS file per event, upload changed files to FTP and return the public links."""
    os.makedirs(ICS_LOCAL_DIR, exist_ok=True)
    df = df_eventliste.rename(columns=str.lower)
    links = []
    seen_ids = set()
    for event in df.to_dict(orient="records"):
        if not is_filled(event["id"]) or not is_filled(event["datum"]):
            links.append(None)
            continue
        event_id = str(int(event["id"])) if isinstance(event["id"], float) else str(event["id"]).strip()
        if event_id in seen_ids:
            logging.warning("Duplicate event id %s, skipping ICS file", event_id)
            links.append(None)
            continue
        seen_ids.add(event_id)
        event["id"] = event_id

        filename = f"event_{event_id}.ics"
        path = os.path.join(ICS_LOCAL_DIR, filename)
        with open(path, "w", encoding="utf-8", newline="") as f:
            f.write(build_ics(event))
        if common.change_tracking.has_changed(path):
            common.upload_ftp(path, remote_path=ICS_FTP_FOLDER)
            common.change_tracking.update_hash_file(path)
        links.append(ICS_URL_BASE + filename)
    return pd.Series(links, index=df_eventliste.index)


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
    df_erwartete_besucherzahl = pd.read_excel(file_path, sheet_name="Erwartete_Besucherzahl")
    df_erwartete_besucherzahl = df_erwartete_besucherzahl[["Datum", "Kategorie"]]
    # Remove the original columns that are no longer needed
    df_anreiseempf.drop(columns=["Bilder", "Weiterfuehrende Links", "Text"], inplace=True)
    columns_by_lower = {col.lower(): col for col in df_eventliste.columns}
    missing = [col for col in COLUMNS_OF_INTEREST_EVENTLISTE if col not in columns_by_lower]
    if missing:
        raise KeyError(f"Missing columns in sheet 'Eventliste': {missing}")
    df_eventliste = df_eventliste[[columns_by_lower[col] for col in COLUMNS_OF_INTEREST_EVENTLISTE]].copy()
    for col in (columns_by_lower["beginn"], columns_by_lower["ende"]):
        df_eventliste[col] = df_eventliste[col].apply(format_time)
    df_eventliste["ICS_Link"] = create_ics_files(df_eventliste)
    df_zeitraum_info.drop(columns=["Text"], inplace=True)

    os.makedirs("data", exist_ok=True)

    path_eventliste = os.path.join("data", "eventliste_stjakob.csv")
    path_anreiseempf = os.path.join("data", "anreiseempfehlung_stjakob.csv")
    path_zeitraum_info = os.path.join("data", "zeitraum_info_stjakob.csv")
    path_erwartete_besucherzahl = os.path.join("data", "erwartete_besucherzahl_stjakob.csv")
    df_eventliste.to_csv(path_eventliste, index=False)
    df_anreiseempf.to_csv(path_anreiseempf, index=False)
    df_zeitraum_info.to_csv(path_zeitraum_info, index=False)
    df_erwartete_besucherzahl.to_csv(path_erwartete_besucherzahl, index=False)
    common.update_ftp_and_odsp(path_eventliste, "kapo/eventverkehr_st.jakob", "100419")
    common.update_ftp_and_odsp(path_anreiseempf, "kapo/eventverkehr_st.jakob", "100429")
    common.update_ftp_and_odsp(path_zeitraum_info, "kapo/eventverkehr_st.jakob", "100464")
    common.update_ftp_and_odsp(path_erwartete_besucherzahl, "kapo/eventverkehr_st.jakob", "100418")

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
    token = get_graph_token()
    site_id = get_site_id(token)
    download_sharepoint_files(token, site_id)
    process_data()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
