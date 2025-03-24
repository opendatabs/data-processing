import os
import csv
import uuid
import datetime
import pytz
import logging
import re
import hashlib

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Path configuration
data_dir = 'data'
output_ics_file = os.path.join(os.path.dirname(__file__), 'data', 'SchulferienBS.ics')
template_file = os.path.join(os.path.dirname(__file__), 'SchulferienBS.ics.template')
csv_files = [
    os.path.join(data_dir, 'school_holidays_since_2025.csv'),
    os.path.join(data_dir, 'school_holidays_since_2024.csv')
]

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
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line == "BEGIN:VEVENT":
                    current_event = {}
                elif line == "END:VEVENT":
                    if event_uid and (event_summary or event_start or event_end):
                        existing_events[event_uid] = {
                            'summary': event_summary,
                            'start': event_start,
                            'end': event_end
                        }
                    current_event = None
                    event_uid = event_summary = event_start = event_end = None
                elif current_event is not None:
                    if line.startswith("UID:"):
                        event_uid = line[4:]
                    elif line.startswith("SUMMARY:"):
                        event_summary = line[8:]
                    elif line.startswith("DTSTART;VALUE=DATE:"):
                        event_start = line[19:]
                    elif line.startswith("DTEND;VALUE=DATE:"):
                        event_end = line[17:]
    except Exception as e:
        logging.error(f"Error parsing existing ICS file: {e}")
        return {}
    
    return existing_events

# Read the template
with open(template_file, 'r', encoding='utf-8') as f:
    template_content = f.read()

# Get current time in UTC format for DTSTAMP
zurich_tz = pytz.timezone('Europe/Zurich')
now_zurich = zurich_tz.localize(datetime.datetime.now())
now_utc = now_zurich.astimezone(pytz.UTC)
dtstamp = now_utc.strftime("%Y%m%dT%H%M%SZ")

# Start with the template content, but remove the sample event
lines = template_content.splitlines()
ics_content = []
for line in lines:
    if not (line.startswith('BEGIN:VEVENT') or 
            line.startswith('END:VEVENT') or 
            line.startswith('DTSTAMP:') or 
            line.startswith('DTSTART;') or 
            line.startswith('DTEND;') or 
            line.startswith('SUMMARY:') or 
            line.startswith('UID:') or
            line.startswith('LOCATION:')):
        ics_content.append(line)

# Collect all events from CSV files
all_events = []
for csv_file in csv_files:
    if os.path.exists(csv_file):
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f, delimiter=';')
            next(reader)  # Skip header
            for row in reader:
                year, name, start_date, end_date = row
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
                
                all_events.append((year, name, start_date_formatted, end_date_formatted))

# Sort events by start date
all_events.sort(key=lambda x: x[2])

# Check if events already exist in the ICS file
existing_events = parse_existing_ics(output_ics_file)
logging.info(f"Found {len(existing_events)} existing events in the ICS file")

# Track which events we've processed to avoid duplicates
processed_events = set()

# Add events to ICS content
for year, name, start_date, end_date in all_events:
    # Generate a deterministic UID for this event
    event_uid = generate_event_uid(year, name, start_date, end_date)
    
    # Skip if we've already processed this event
    if event_uid in processed_events:
        continue
    processed_events.add(event_uid)
    
    # Check if this event already exists in the ICS file
    if event_uid in existing_events:
        existing_event = existing_events[event_uid]
        # If the event exists with the same details, no need to update
        if existing_event['summary'] == name and existing_event['start'] == start_date and existing_event['end'] == end_date:
            logging.info(f"Event '{name}' ({start_date} to {end_date}) already exists with the same details - skipping")
            continue
        else:
            logging.info(f"Event '{name}' exists but details have changed - updating")
    
    # Create the event block
    event_block = [
        "",  # Add blank line before event
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

# Add final newline and END:VCALENDAR
ics_content.extend(["", "END:VCALENDAR", ""])

# Create the final ICS content
final_ics_content = '\n'.join(ics_content)

# Write to output file
os.makedirs(os.path.dirname(output_ics_file), exist_ok=True)
with open(output_ics_file, 'w', encoding='utf-8') as f:
    f.write(final_ics_content)

logging.info(f"ICS file created successfully at {output_ics_file}")
logging.info("TODO: Upload the ICS file to the appropriate location")
