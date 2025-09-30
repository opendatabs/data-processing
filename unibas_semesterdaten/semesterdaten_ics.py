import os
import uuid
import logging
import pandas as pd
from datetime import datetime, timezone
import common  


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

tzid = "Europe/Zurich"
BASE_CSV = "data/100469_unibas_semesterdaten.csv"
BASE_OUT = "data/unibas_semesterdaten"


def categorize(name: str) -> str:
    if name == "Vorlesungen":
        return "vorlesungen"
    if name == "Akademisches Semester":
        return "akademisches_semester"
    return "freizeit_feiertage"

def ics_header(calname: str, caldesc: str) -> str:
    # Calendar header
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-// Universität Basel//DE",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{calname}",
        f"X-WR-TIMEZONE:{tzid}",
        f"X-WR-CALDESC:{caldesc}",
    ]
    return "\r\n".join(lines) + "\r\n"


def make_uid(row) -> str:
    # create UID based on unique combination of fields
    base = f"{row['semester']}-{row['jahr']}-{row['name']}-{row['startdatum']}-{row['enddatum']}"
    return f"{uuid.uuid5(uuid.NAMESPACE_URL, base)}@unibas"

def write_ics_from_df(df_subset: pd.DataFrame, output_ics: str, calname: str, caldesc: str) -> None:
    # Current UTC timestamp for DTSTAMP
    dtstamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    count = 0

    with open(output_ics, "wb") as out:
        out.write(ics_header(calname, caldesc).encode("utf-8"))

        for _, row in df_subset.iterrows():
            # All-day events: DTEND exclusive, therefore +1 day
            dtstart = row["startdatum"]
            dtend_excl = row["enddatum"]

            summary = f"{row['name']} ({row['semester']} {row['jahr']})" if row["name"] != "Vorlesungen" else f"Vorlesungen ({row['semester']} {row['jahr']})"
            uid = make_uid(row)

            event_lines = [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART;VALUE=DATE:{dtstart}",
                f"DTEND;VALUE=DATE:{dtend_excl}",
                f"SUMMARY:{summary}",
                "TRANSP:TRANSPARENT",
                "END:VEVENT",
            ]
            out.write(("\r\n".join(event_lines) + "\r\n").encode("utf-8"))
            count += 1

        out.write("END:VCALENDAR\r\n".encode("utf-8"))

    logging.info(f"ICS erstellt: {os.path.abspath(output_ics)} mit {count} Events")
    # FTP-Upload 
    common.upload_ftp(filename=output_ics, remote_path="ed/hochschulen")

def main():
    df = pd.read_csv(BASE_CSV, sep=";")
    logging.info(f"CSV geladen: {len(df)} Zeilen")
    df["kategorie"] = df["name"].apply(categorize)
    df["startdatum"] = pd.to_datetime(df["startdatum"], format="%Y-%m-%d").dt.strftime("%Y%m%d")
    df["enddatum"]   = (pd.to_datetime(df["enddatum"], format="%Y-%m-%d") + pd.Timedelta(days=1)).dt.strftime("%Y%m%d")


    # 4 Kalender
    calendars = [
        # Alle zusammen
        (
            df,
            f"{BASE_OUT}.ics",
            "Semesterdaten Universität Basel — Alle",
            "Vorlesungen, Akademisches Semester, Feiertage und vorlesungsfreie Zeiten",
        ),
        # Vorlesungen
        (
            df[df["kategorie"] == "vorlesungen"],
            f"{BASE_OUT}_vorlesungen.ics",
            "Semesterdaten Universität Basel — Vorlesungen",
            "Vorlesungszeiträume",
        ),
        # Akademisches Semester
        (
            df[df["kategorie"] == "akademisches_semester"],
            f"{BASE_OUT}_akademisches_semester.ics",
            "Semesterdaten Universität Basel — Akademisches Semester",
            "Akademische Semesterzeiträume",
        ),
        # Feiertage und vorlesungsfreie Zeiten
        (
            df[df["kategorie"] == "freizeit_feiertage"],
            f"{BASE_OUT}_freizeit_feiertage.ics",
            "Semesterdaten Universität Basel — Feiertage und vorlesungsfreie Zeiten",
            "Feiertage und vorlesungsfreie Zeiten (z. B. Weihnachten, Fasnacht, Ostern)",
        ),
    ]

    for subset, out_path, name, desc in calendars:
        if subset.empty:
            logging.warning(f"Übersprungen (leer): {out_path}")
            continue
        write_ics_from_df(subset, out_path, name, desc)

if __name__ == "__main__":
    main()
    logging.info("Alle Kalender verarbeitet")