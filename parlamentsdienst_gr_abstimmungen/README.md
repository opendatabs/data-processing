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
- **FTP Directory Listing Management:**
  - Manages the retrieval and storage of FTP directory listings for various data types.
- **Recursive Directory Processing:**
  - Implements a recursive directory listing function for deep FTP directory structures.
