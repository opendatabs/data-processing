import os
import csv
import uuid
import datetime
import pytz
import logging
import re
import hashlib
import glob

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Path configuration
data_dir = 'data'
output_ics_file = os.path.join(os.path.dirname(__file__), 'data', 'SchulferienBS.ics')
template_file = os.path.join(os.path.dirname(__file__), 'SchulferienBS.ics.template')

# Year range configuration
# Based on the template description: "Zur Referenz sind vergangene Ferien des aktuellen und vorherigen Schuljahres 
# sowie zukünftige Ferien für ungefähr 3 Jahre im Voraus enthalten."
current_year = datetime.datetime.now().year
YEAR_RANGE_START = current_year - 2  # Previous school year (better more than not enough)
YEAR_RANGE_END = current_year + 3    # Approximately 3 years in the future
# IMPORTANT: If these year ranges are modified, make sure to update the description in the SchulferienBS.ics.template file as well

# Function to extract year from CSV filename
def extract_year_from_filename(filename):
    match = re.search(r'school_holidays_since_(\d+)\.csv', os.path.basename(filename))
    if match:
        return int(match.group(1))
    logging.warning(f"No year found in filename: {filename}")
    return 0  # Default for files that don't match the pattern

# Function to generate a deterministic UID for an event
def generate_event_uid(year, name, start_date, end_date):
    # Create a unique string combining all event properties
    event_str = f"{year}_{name}_{start_date}_{end_date}"
    # Create a hash and use it as the basis for the UUID
    hash_obj = hashlib.md5(event_str.encode())
    # Convert to a UUID format
    return str(uuid.UUID(hex=hash_obj.hexdigest()))

# Function to parse existing ICS file if it exists
def parse_existing_ics(file_path):
    if not os.path.exists(file_path):
        return {}
    
    existing_events = {}
    current_event = None
    event_uid = None
    event_summary = None
    event_start = None
    event_end = None
    event_dtstamp = None
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line == "BEGIN:VEVENT":
                    current_event = {}
                elif line == "END:VEVENT":
                    if event_uid and (event_summary or event_start or event_end):
                        existing_events[event_uid] = {
                            'summary': event_summary.strip() if event_summary else None,
                            'start': event_start,
                            'end': event_end,
                            'dtstamp': event_dtstamp
                        }
                    current_event = None
                    event_uid = event_summary = event_start = event_end = event_dtstamp = None
                elif current_event is not None:
                    if line.startswith("UID:"):
                        event_uid = line[4:]
                    elif line.startswith("SUMMARY:"):
                        event_summary = line[8:]
                    elif line.startswith("DTSTART;VALUE=DATE:"):
                        event_start = line[19:]
                    elif line.startswith("DTEND;VALUE=DATE:"):
                        event_end = line[17:]
                    elif line.startswith("DTSTAMP:"):
                        event_dtstamp = line[8:]
    except Exception as e:
        logging.error(f"Error parsing existing ICS file: {e}")
        return {}
    
    return existing_events

def main():
    """Main function to generate the ICS file"""
    # Dynamically find all CSV files in the data directory
    data_dir_abs = os.path.join(os.path.dirname(__file__), data_dir)
    csv_files = glob.glob(os.path.join(data_dir_abs, '*.csv'))
    # Exclude any dummy files or files we don't want to process
    csv_files = [f for f in csv_files if 'dummy' not in os.path.basename(f)]

    # Sort files by year (descending) so newer files are processed first
    csv_files.sort(key=extract_year_from_filename, reverse=True)

    logging.info(f"Found {len(csv_files)} CSV files in the data directory{':' if csv_files else '.'}")
    for csv_file in csv_files:
        year = extract_year_from_filename(csv_file)
        logging.info(f"- {csv_file} (year: {year})")

    # Read the template and get just the header (everything up to BEGIN:VEVENT)
    template_lines = []
    with open(template_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip() == "BEGIN:VEVENT":
                break
            template_lines.append(line.strip())

    # Get current time in UTC format for DTSTAMP
    zurich_tz = pytz.timezone('Europe/Zurich')
    now_zurich = zurich_tz.localize(datetime.datetime.now())
    now_utc = now_zurich.astimezone(pytz.UTC)
    dtstamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

    # Use the template header as the base for our content
    ics_content = template_lines

    # Collect all events from CSV files
    all_events = []
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=';')
                next(reader)  # Skip header
                for row in reader:
                    year, name, start_date, end_date = row
                    # Strip whitespace from the name
                    name = name.strip()
                    # Remove any time portion and convert to proper format for iCalendar (YYYYMMDD)
                    start_date = start_date.split(' ')[0]
                    end_date = end_date.split(' ')[0]
                    start_date_formatted = start_date.replace('-', '')
                    end_date_formatted = end_date.replace('-', '')

                    # Make sure that the dates are exactly 8 characters long and numbers only, if not, skip the event
                    if len(start_date_formatted) != 8 or not start_date_formatted.isdigit():
                        logging.warning(f"Invalid start date: {start_date_formatted}")
                        continue
                    if len(end_date_formatted) != 8 or not end_date_formatted.isdigit():
                        logging.warning(f"Invalid end date: {end_date_formatted}")
                        continue
                    
                    # Extract the year from the start date to filter events by year range
                    event_year = int(start_date_formatted[:4])
                    
                    # Filter events to only include those in the configured year range
                    if YEAR_RANGE_START <= event_year <= YEAR_RANGE_END:
                        all_events.append((year, name, start_date_formatted, end_date_formatted))
                    else:
                        logging.debug(f"Skipping event outside year range: {name} ({start_date_formatted} to {end_date_formatted})")

    # Sort events by start date
    all_events.sort(key=lambda x: x[2])

    # Check if events already exist in the ICS file
    existing_events = parse_existing_ics(output_ics_file)
    logging.info(f"Found {len(existing_events)} existing events in the ICS file")

    # Track which events we've processed to avoid duplicates
    processed_events = set()
    skipped_events_count = 0
    updated_events_count = 0
    deleted_events_count = 0

    # Add events to ICS content
    for year, name, start_date, end_date in all_events:
        # Strip whitespace from the name
        name = name.strip()
        
        # Generate a deterministic UID for this event
        event_uid = generate_event_uid(year, name, start_date, end_date)
        
        # Skip if we've already processed this event
        if event_uid in processed_events:
            continue
        processed_events.add(event_uid)
        
        # Check if this event already exists in the ICS file
        if event_uid in existing_events:
            existing_event = existing_events[event_uid]
            existing_summary = existing_event['summary'].strip() if existing_event['summary'] else ""
            
            # If the event exists with the same details, no need to update
            if existing_summary == name and existing_event['start'] == start_date and existing_event['end'] == end_date:
                logging.debug(f"Event '{name}' ({start_date} to {end_date}) already exists with the same details - skipping")
                skipped_events_count += 1
                
                # Use the original timestamp for unchanged events
                event_timestamp = existing_event['dtstamp'] if existing_event['dtstamp'] else dtstamp
                
                # We still need to add the existing event to the output
                event_block = [
                    "BEGIN:VEVENT",
                    f"DTSTAMP:{event_timestamp}",
                    f"DTSTART;VALUE=DATE:{start_date}",
                    f"DTEND;VALUE=DATE:{end_date}",
                    f"SUMMARY:{name}",
                    f"UID:{event_uid}",
                    "LOCATION:Basel-Stadt, Schweiz",
                    "END:VEVENT"
                ]
                ics_content.extend(event_block)
                continue
            else:
                logging.info(f"Event '{name}' exists but details have changed - updating")
                # The event will be updated by proceeding with the code below
                updated_events_count += 1
        
        # Create the event block
        event_block = [
            "BEGIN:VEVENT",
            f"DTSTAMP:{dtstamp}",
            f"DTSTART;VALUE=DATE:{start_date}",
            f"DTEND;VALUE=DATE:{end_date}",
            f"SUMMARY:{name}",
            f"UID:{event_uid}",
            "LOCATION:Basel-Stadt, Schweiz",
            "END:VEVENT"
        ]
        ics_content.extend(event_block)

    # Add any existing events that weren't in the CSV files
    preserved_events_count = 0
    for uid, event in existing_events.items():
        if uid not in processed_events:
            # Check if the event's year is within the configured range
            event_year = int(event['start'][:4])
            if YEAR_RANGE_START <= event_year <= YEAR_RANGE_END:
                summary = event['summary'].strip() if event['summary'] else ""
                
                # Use the original timestamp for preserved events
                event_timestamp = event['dtstamp'] if event['dtstamp'] else dtstamp
                
                event_block = [
                    "BEGIN:VEVENT",
                    f"DTSTAMP:{event_timestamp}",
                    f"DTSTART;VALUE=DATE:{event['start']}",
                    f"DTEND;VALUE=DATE:{event['end']}",
                    f"SUMMARY:{summary}",
                    f"UID:{uid}",
                    "LOCATION:Basel-Stadt, Schweiz",
                    "END:VEVENT"
                ]
                ics_content.extend(event_block)
                preserved_events_count += 1
            else:
                logging.debug(f"Deleting old event outside year range: {event['summary']} ({event['start']} to {event['end']})")
                deleted_events_count += 1

    # Add final newline and END:VCALENDAR
    ics_content.extend(["END:VCALENDAR"])

    # Create the final ICS content
    final_ics_content = '\n'.join(ics_content)

    # Write to output file
    os.makedirs(os.path.dirname(output_ics_file), exist_ok=True)
    with open(output_ics_file, 'w', encoding='utf-8') as f:
        f.write(final_ics_content)

    # Log a summary of what happened
    if skipped_events_count > 0:
        logging.info(f"Skipped {skipped_events_count} events that already existed with the same details")
    if updated_events_count > 0:
        logging.info(f"Updated {updated_events_count} events that had changed details")
    if preserved_events_count > 0:
        logging.info(f"Preserved {preserved_events_count} existing events that were not in the CSV files")
    if deleted_events_count > 0:
        logging.info(f"Deleted {deleted_events_count} old events outside the configured year range")
        
    logging.info(f"Added {len(processed_events) - skipped_events_count - updated_events_count} new events")
    logging.info(f"ICS file contains events from {YEAR_RANGE_START} to {YEAR_RANGE_END}")
    logging.info(f"ICS file created successfully at {output_ics_file}")

    return output_ics_file

if __name__ == "__main__":
    main()
