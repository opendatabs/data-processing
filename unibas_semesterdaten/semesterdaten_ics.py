import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import common
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def to_yyyymmdd(date_str: str) -> str:
    # Expect "YYYY-MM-DD" → return "YYYYMMDD"
    return datetime.strptime(date_str.strip(), "%Y-%m-%d").strftime("%Y%m%d")


def add_one_day_yyyymmdd(yyyymmdd: str) -> str:
    # Add one day because DTEND is exclusive in iCalendar
    d = datetime.strptime(yyyymmdd, "%Y%m%d") + timedelta(days=1)
    return d.strftime("%Y%m%d")


def deterministic_uid(event_str: str) -> str:
    # UUID v5 with SHA-1, stable for identical input
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, event_str))


def main():
    input_csv = "data/100469_unibas_semesterdaten.csv"
    output_ics = "data/100469_unibas_semesterdaten.ics"

    calname = "Semesterdaten Universität Basel"
    caldesc = (
        "Offizielle Semesterdaten und vorlesungsfreie Zeiten der Universität Basel, "
        "basierend auf den veröffentlichten Terminen der Universität Basel."
    )
    tzid = "Europe/Zurich"

    # Calendar header
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Open Data Basel-Stadt//semesterdaten-export//DE",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{calname}",
        f"X-WR-TIMEZONE:{tzid}",
        f"X-WR-CALDESC:{caldesc}",
    ]

    # Current UTC timestamp for DTSTAMP
    dtstamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    try:
        if not os.path.exists(input_csv):
            logging.error(f"CSV file not found: {os.path.abspath(input_csv)}")
            return None

        logging.info(f"Reading CSV file: {os.path.abspath(input_csv)}")
        df = pd.read_csv(input_csv, sep=";", encoding="utf-8")
        raw_rows = len(df)
        logging.info(f"Total rows in CSV: {raw_rows}")

        # Check required columns (lowercase)
        need_cols = {"name", "startdatum", "enddatum"}
        missing = need_cols - set(df.columns)
        if missing:
            logging.error(f"Missing required columns: {', '.join(sorted(missing))}")
            return None

        # Drop invalid rows and sort by start date
        before = len(df)
        df = df.dropna(subset=["name", "startdatum", "enddatum"]).sort_values("startdatum", kind="stable")
        after = len(df)
        logging.info(f"Valid rows: {after} (dropped: {before - after})")

        # Build events
        event_count = 0
        for _, row in df.iterrows():
            semester = str(row.get("semester", "")).strip()
            jahr = str(row.get("jahr", "")).strip()
            name = str(row["name"]).strip()
            start_raw = str(row["startdatum"]).strip()
            end_raw = str(row["enddatum"]).strip()

            try:
                dtstart = to_yyyymmdd(start_raw)
                dtend_inclusive = to_yyyymmdd(end_raw)
            except ValueError as e:
                logging.warning(f"Skipping row due to invalid date [{name}, {start_raw}, {end_raw}]: {e}")
                continue

            dtend_exclusive = add_one_day_yyyymmdd(dtend_inclusive)

            # Event title: Name – Semester – Year (if present)
            parts = [name]
            if semester:
                parts.append(semester)
            if jahr:
                parts.append(jahr)
            summary = " – ".join(parts)

            uid = deterministic_uid(f"{semester}_{jahr}_{name}_{dtstart}_{dtend_inclusive}")

            event = [
                "BEGIN:VEVENT",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART:{dtstart}",
                f"DTEND:{dtend_exclusive}",
                f"SUMMARY:{summary}",
                f"UID:{uid}",
                "TRANSP:TRANSPARENT",
                "STATUS:CONFIRMED",
                "END:VEVENT",
            ]
            lines.extend(event)
            event_count += 1

        lines.append("END:VCALENDAR")

        # Write final ICS file
        os.makedirs(os.path.dirname(output_ics) or ".", exist_ok=True)
        with open(output_ics, "wb") as out:
            out.write(("\r\n".join(lines) + "\r\n").encode("utf-8"))

        logging.info(f"ICS file created: {os.path.abspath(output_ics)}")
        logging.info(f"Number of events: {event_count}")

        # Upload to FTP
        common.upload_ftp(filename=output_ics, remote_path="hochschulen")

    except Exception as e:
        logging.exception(f"Error while generating ICS: {e}")
        return None


if __name__ == "__main__":
    main()
