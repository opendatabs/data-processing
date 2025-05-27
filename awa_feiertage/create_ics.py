import csv
import datetime
import glob
import hashlib
import logging
import os
import re
import uuid

import pytz

# Logging setup
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Paths
data_dir = "data"
output_ics_file = os.path.join("data", "FreiUndFeiertage.ics")
template_file = os.path.join("FreiUndFeiertage.ics.template")

# Date for DTSTAMP
zurich_tz = pytz.timezone("Europe/Zurich")
now_zurich = zurich_tz.localize(datetime.datetime.now())
now_utc = now_zurich.astimezone(pytz.UTC)
dtstamp = now_utc.strftime("%Y%m%dT%H%M%SZ")


def extract_year_from_filename(filename):
    match = re.search(
        r"frei-und-feiertage(?:_(\d{4}))?\.csv", os.path.basename(filename)
    )
    return int(match.group(1)) if match and match.group(1) else 0


def generate_event_uid(year, name, date):
    event_str = f"{year}_{name}_{date}"
    hash_obj = hashlib.md5(event_str.encode())
    return str(uuid.UUID(hex=hash_obj.hexdigest()))


def parse_existing_ics(file_path):
    if not os.path.exists(file_path):
        return {}

    existing_events = {}
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line == "BEGIN:VEVENT":
                    current_event = {}
                elif line.startswith("SUMMARY:"):
                    current_event["summary"] = line[8:]
                elif line.startswith("DTSTART"):
                    current_event["start"] = line.split(":")[1]
                elif line.startswith("DTEND"):
                    current_event["end"] = line.split(":")[1]
                elif line.startswith("UID:"):
                    uid = line[4:]
                elif line.startswith("DTSTAMP:"):
                    current_event["dtstamp"] = line[8:]
                elif line == "END:VEVENT":
                    if uid:
                        existing_events[uid] = current_event
    except Exception as e:
        logging.error(f"Failed to parse existing ICS: {e}")
    return existing_events


def main():
    # Load template header
    template_lines = []
    with open(template_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() == "BEGIN:VEVENT":
                break
            template_lines.append(line.strip())

    ics_content = template_lines
    existing_events = parse_existing_ics(output_ics_file)
    processed_uids = set()

    csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
    csv_files = [f for f in csv_files if "dummy" not in os.path.basename(f)]

    for csv_file in csv_files:
        logging.info(f"Processing file: {csv_file}")
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=",")
            next(reader)  # skip header
            for row in reader:
                _, date_str_raw, name, _ = row
                name = name.strip()
                year = date_str_raw.split("-")[0]
                date_str = date_str_raw.replace("-", "")

                if len(date_str) != 8 or not date_str.isdigit():
                    logging.warning(f"Skipping invalid date: {date_str}")
                    continue

                # Compute next day for DTEND
                start_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
                end_date = start_date + datetime.timedelta(days=1)
                end_date_str = end_date.strftime("%Y%m%d")

                uid = generate_event_uid(year, name, date_str)
                if uid in processed_uids:
                    continue
                processed_uids.add(uid)

                event_dtstamp = dtstamp
                exists = existing_events.get(uid)
                if exists:
                    if (
                        exists["summary"] == name
                        and exists["start"] == date_str
                        and exists["end"] == end_date_str
                    ):
                        event_dtstamp = exists.get("dtstamp", dtstamp)
                        logging.debug(f"Skipping unchanged event: {name}")
                    else:
                        logging.info(f"Updating event: {name}")

                ics_content.extend(
                    [
                        "BEGIN:VEVENT",
                        f"DTSTAMP:{event_dtstamp}",
                        f"DTSTART;VALUE=DATE:{date_str}",
                        f"DTEND;VALUE=DATE:{end_date_str}",
                        f"SUMMARY:{name}",
                        f"UID:{uid}",
                        "LOCATION:Basel-Stadt, Schweiz",
                        "END:VEVENT",
                    ]
                )

    ics_content.append("END:VCALENDAR")

    os.makedirs(os.path.dirname(output_ics_file), exist_ok=True)
    with open(output_ics_file, "wb") as f:
        for line in ics_content:
            f.write(f"{line}\r\n".encode("utf-8"))

    logging.info(f"ICS file written to: {output_ics_file}")
    return output_ics_file


if __name__ == "__main__":
    main()
