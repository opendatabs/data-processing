import csv
import datetime
import hashlib
import logging
import os
import pytz
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Path configuration
data_dir = "data"
output_ics_file = os.path.join(os.path.dirname(__file__), "data", "SchulferienBS.ics")
template_file = os.path.join(os.path.dirname(__file__), "SchulferienBS.ics.template")
csv_file_name = "schulferienBS.csv"

# Year range configuration
# Based on the template description: "Zur Referenz sind vergangene Ferien des aktuellen und vorherigen Schuljahres
# sowie zukünftige Ferien für ungefähr 3 Jahre im Voraus enthalten."
current_year = datetime.datetime.now().year
YEAR_RANGE_START = current_year - 2  # Previous school year (better more than not enough)
YEAR_RANGE_END = current_year + 3  # Approximately 3 years in the future
# IMPORTANT: If these year ranges are modified, make sure to update the description in the SchulferienBS.ics.template file as well

# Events that should be included in the ICS file
ICS_INCLUDE_EVENTS = [
    "Herbstferien", 
    "Weihnachtsferien", 
    "Fasnachts- und Sportferien",
    "Frühjahrsferien",
    "Sommerferien",
    "Schulfrei (1. Mai)",
    "Schulfrei (Auffahrt)",
    "Schulfrei (Pfingstmontag)"
]


# Function to add one day to a date string in YYYYMMDD format
def add_one_day(date_str):
    date_obj = datetime.datetime.strptime(date_str, "%Y%m%d")
    next_day = date_obj + datetime.timedelta(days=1)
    return next_day.strftime("%Y%m%d")


# Function to generate a deterministic UID for an event
def generate_event_uid(year, name, start_date, end_date):
    # Create a unique string combining all event properties
    event_str = f"{year}_{name}_{start_date}_{end_date}"
    # Create a hash and use it as the basis for the UUID
    hash_obj = hashlib.md5(event_str.encode())
    # Convert to a UUID format
    return str(uuid.UUID(hex=hash_obj.hexdigest()))


def main():
    """Main function to generate the ICS file from scratch"""
    # Path to the CSV file
    data_dir_abs = os.path.join(os.path.dirname(__file__), data_dir)
    csv_file_path = os.path.join(data_dir_abs, csv_file_name)

    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file not found: {csv_file_path}")
        return None

    logging.info(f"Using CSV file: {csv_file_path}")

    # Read the template and get just the header (everything up to BEGIN:VEVENT)
    template_lines = []
    with open(template_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip() == "BEGIN:VEVENT":
                break
            template_lines.append(line.strip())

    # Get current time in UTC format for DTSTAMP
    zurich_tz = pytz.timezone("Europe/Zurich")
    now_zurich = zurich_tz.localize(datetime.datetime.now())
    now_utc = now_zurich.astimezone(pytz.UTC)
    dtstamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

    # Use the template header as the base for our content
    ics_content = template_lines

    # Collect all events from the CSV file
    all_events = []
    
    with open(csv_file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader)  # Skip header
        for row in reader:
            year, name, start_date, end_date = row
            
            # Include only events that should be in the ICS file
            if name not in ICS_INCLUDE_EVENTS:
                logging.debug(f"Skipping event not in include list: {name}")
                continue
            
            # Remove any time portion and convert to proper format for iCalendar (YYYYMMDD)
            start_date = start_date.split(" ")[0]
            end_date = end_date.split(" ")[0]
            start_date_formatted = start_date.replace("-", "")
            end_date_formatted = end_date.replace("-", "")

            # Extract the year from the start date to filter events by year range
            event_year = int(start_date_formatted[:4])

            # Filter events to only include those in the configured year range
            if YEAR_RANGE_START <= event_year <= YEAR_RANGE_END:
                all_events.append((year, name, start_date_formatted, end_date_formatted))
            else:
                logging.debug(
                    f"Skipping event outside year range: {name} ({start_date_formatted} to {end_date_formatted})"
                )

    # Sort events by start date
    all_events.sort(key=lambda x: x[2])

    # Add events to ICS content
    event_count = 0
    for year, name, start_date, end_date in all_events:
        # Generate a deterministic UID for this event
        event_uid = generate_event_uid(year, name, start_date, end_date)
        
        # Add one day to end date for exclusive range
        end_date_exclusive = add_one_day(end_date)

        # Create the event block
        event_block = [
            "BEGIN:VEVENT",
            f"DTSTAMP:{dtstamp}",
            f"DTSTART:{start_date}",
            f"DTEND:{end_date_exclusive}",
            f"SUMMARY:{name}",
            f"UID:{event_uid}",
            "LOCATION:Basel-Stadt, Schweiz",
            "END:VEVENT",
        ]
        ics_content.extend(event_block)
        event_count += 1

    # Add final newline and END:VCALENDAR
    ics_content.extend(["END:VCALENDAR"])

    # Create the final ICS content with proper CRLF line endings (RFC 5545)
    # Write to output file in binary mode to avoid platform-specific newline handling
    os.makedirs(os.path.dirname(output_ics_file), exist_ok=True)
    with open(output_ics_file, "wb") as f:
        for line in ics_content:
            f.write(f"{line}\r\n".encode("utf-8"))

    logging.info(f"Created new ICS file with {event_count} events")
    logging.info(f"ICS file contains events from {YEAR_RANGE_START} to {YEAR_RANGE_END}")
    logging.info(f"ICS file created successfully at {output_ics_file}")

    return output_ics_file


if __name__ == "__main__":
    main()
