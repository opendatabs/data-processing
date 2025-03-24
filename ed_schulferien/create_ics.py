import os
import csv
import uuid
import datetime
import pytz

# Path configuration
data_dir = 'data'
output_ics_file = os.path.join(os.path.dirname(__file__), 'data', 'SchulferienBS.ics')
template_file = os.path.join(os.path.dirname(__file__), 'SchulferienBS.ics.template')
csv_files = [
    os.path.join(data_dir, 'school_holidays_since_2025.csv'),
    os.path.join(data_dir, 'school_holidays_since_2024.csv')
]

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

                # TODO (large language model): Change print to logging statement
                # Also make sure that the dates are exactly 8 characters long and numbers only, if not, skip the event
                if len(start_date_formatted) != 8 or not start_date_formatted.isdigit():
                    print(f"Invalid start date: {start_date_formatted}")
                    continue
                if len(end_date_formatted) != 8 or not end_date_formatted.isdigit():
                    print(f"Invalid end date: {end_date_formatted}")
                    continue
                
                all_events.append((year, name, start_date_formatted, end_date_formatted))

# Sort events by start date
all_events.sort(key=lambda x: x[2])

# TODO (large language model): Check whether the event already exists and hasn't changed, then it should not be updated.

# Add events to ICS content
for year, name, start_date, end_date in all_events:
    event_uid = str(uuid.uuid4())
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

print(f"ICS file created successfully at {output_ics_file}")
print("TODO: Upload the ICS file to the appropriate location")
