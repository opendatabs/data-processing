## Expanded Process Description

This job processes various data sources and performs multiple operations as outlined below:

## Tagesordnungen (Traktandenlisten) - `handle_tagesordnungen()`
- Processes contents of one `*.txt` (CSV) file for each session day, containing details of each Traktandum. These can be linked to a single poll, to none, or to multiple Geschäfte and Dokumente.
- Manages a folder of CSV files containing all past and present Tagesordnungen.
- The file for the current session may only be present after the first poll of the session has been completed.

## Session Calendar - `get_session_calendar()`
- Retrieves iCal data from Google Calendar, with one entry per session day.
- Handles the updating and storage of this calendar data in a local file system.

## New Election System Polls - `handle_polls_json()`
- Processes poll data from the new election system of GRIBS (Grossratsinformationssystem Basel-Stadt).
- Involves handling JSON files, including parsing and data transformation.

## Past Polls from FTP Server - `handle_polls_xml()`
- Handles contents and filenames similar to live polls, but for historical data before August 2023.
- Organizes a folder with a subfolder structure for PDF files, and a flat folder structure for all XML files.

## Congress Center Polls - `handle_congress_center_polls()`
- Processes specific poll data from the period when the Grosser Rat held sessions at the Congress Center Basel.
- Includes handling and parsing of XLSX files specific to this period.

## Additional Operations
- **File Handling and Data Transformation:**
  - Performs file downloading, renaming, and uploading operations, particularly for handling data backup on an FTP server.
  - Includes data cleaning and normalization operations, especially for handling text and Excel data.
- **Session Activity Check:**
  - Determines if a session is currently active using `is_session_now`, considering predefined hours before and after session times.
- **Unique Session Dates Calculation:**
  - Computes unique session dates from calendar data to filter out test polls.
- **Test Polls and Polls without Quorum** (`poll_filters.py`, rules provided by the Parlamentsdienst):
  - Session time (local time) = standard windows 09:00–12:30, 15:00–18:30, 20:00–23:30 **or** a single session of that
    day in the session calendar, from start to end + 30 min (cancelled sessions and all-day events are ignored).
    The calendar covers deviating sessions (e.g. Covid 2020 from 08:00 / 14:00, 2023-12-06 from 14:00); the standard
    windows are kept because the calendar entries from 2022-11 to 2023-03 are one hour too early.
  - Poll outside of session time **and** more than 50 members absent → test poll, not published.
  - Poll inside of session time with more than 50 members absent → no quorum, `Typ` is set to `ungültig`.
  - The time of day is taken from `Zeit`, because `started`/`Zeitstempel` of the JSON system is off by 1–2 hours on some days (`Zeitstempel` is corrected in that case).
- **E-mail report of removed test polls** (`poll_report.py`):
  - If a run removes test polls that have not been reported before, an e-mail (`common.send_email`, receivers from
    `EMAIL_RECEIVERS`) is sent with an Excel file attached (`data/herausgefilterte_testabstimmungen.xlsx`, sheets
    "Neu herausgefiltert", "Alle herausgefiltert", "Regeln").
  - Lookup table of all reported test polls (key `Datum`, `Zeit`, `Abst_Nr`): `change_tracking/herausgefilterte_abstimmungen.csv`.
    It is only updated after the e-mail was sent, so a failed e-mail is retried on the next run; a failure never stops the job.
  - Delete a row (or the file) to have a test poll reported again.

## Output: one file per session day (`day_files.py`)
Dataset 100186 is fed from the FTP folder `parlamentsdienst/gr_abstimmungsergebnisse/100186_daten` (no ODS realtime push anymore):
```
100186_daten/
  Amtsjahr_2013-2014/20140115_archiv_xml.csv
  Amtsjahr_2021-2022/20210203_congress_center.csv
  Amtsjahr_2024-2025/20240417_gribs_json.csv
```
- One subfolder per Amtsjahr (1 February – 31 January), one file per session day, suffix = source system:
  `gribs_json` (current system), `archiv_xml` (old system until 2023-06), `congress_center` (2021-02 – 2022-02).
- All files have the same columns (`day_files.OUTPUT_COLUMNS`). Local copies are in `data/100186_daten`.
- A day file is always rebuilt from all polls of that day at the source, so polls deleted by the Parlamentsdienst disappear.
- Only changed files are uploaded; dataset 100186 is republished once if anything changed.

Modes:
- `uv run -m etl` (during sessions): only the latest session day of the JSON system.
- `uv run -m etl --full` (daily): all session days of the JSON system; day files of days no longer at the source are deleted.
- `uv run -m etl --archive` (on demand): additionally rebuilds the XML archive and the Congress Center files
  (needs `data_orig/congress_center/GR-BS_*.xlsx` locally). Combine with `--full` for a complete rebuild.
- **FTP Directory Listing Management:**
  - Manages the retrieval and storage of FTP directory listings for various data types.
- **Recursive Directory Processing:**
  - Implements a recursive directory listing function for deep FTP directory structures.
