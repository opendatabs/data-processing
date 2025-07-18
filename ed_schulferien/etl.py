import csv
import datetime
import logging
import os
import pathlib

import common
import create_ics
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Constants and configuration
ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100397")

csv_filename = "schulferienBS.csv"
excel_filename = "Schulferien BS.xlsx"

# Default path for local development
data_path = "data/"

# Event names to include in CSV files
CSV_INCLUDE_EVENTS = [
    "Herbstferien",
    "Weihnachtsferien",
    "Fasnachts- und Sportferien",
    "Frühjahrsferien",
    "Sommerferien",
    "Basler Fasnacht",
    "Dreitageblock",
    "1. Mai",
    "Auffahrt",
    "Pfingstmontag",
    "Jahresversammlung der Kantonalen Schulkonferenz",
]


def main():
    # TODO: This is really ugly, but the easiest way I found to make it runnable both locally and on the server
    script_dir = pathlib.Path(__file__).parent.absolute()
    if os.path.basename(script_dir) != "ed_schulferien":
        script_dir = os.path.join(script_dir, "ed_schulferien")

    # Check if we're in Docker environment (where data_orig is mounted)
    if os.path.exists("/code/data_orig"):
        logging.info("Running in Docker environment")
        excel_path = os.path.join("/code/data_orig", excel_filename)
        # Use the mounted data path directly in Docker
        data_path_abs = "/code/data"
    else:
        # Local development path
        logging.info("Running in local development environment")
        excel_path = os.path.join(script_dir, excel_filename)
        data_path_abs = os.path.join(script_dir, data_path)

    # Ensure directories exist
    os.makedirs(data_path_abs, exist_ok=True)

    # Check embargo before proceeding
    if not check_embargo(excel_path):
        logging.warning("Processing aborted due to active embargo")
        return

    # Verify the Excel TEMPLATE sheet
    if not verify_excel(excel_path, "TEMPLATE", is_template=True):
        raise ValueError("Excel TEMPLATE verification failed. Please check the template structure.")

    # Process the Excel file and create a single CSV file with data from all year tabs
    process_excel_file(
        excel_path=excel_path,
        data_path_abs=data_path_abs,
    )

    logging.info(f"Processed all year tabs into a single CSV file: {csv_filename}")

    # Update CSV on FTP
    common.upload_ftp(filename=os.path.join(data_path_abs, csv_filename), remote_path="ed/schulferien")

    # Push CSV data to ODS with realtime push
    push_data_csv_with_realtime_push(data_path_abs=data_path_abs, filename=csv_filename)

    # Update ICS file on FTP server
    update_ics_file_on_ftp_server()


def verify_excel(excel_path: str, sheet_name: str, is_template: bool = False) -> bool:
    """Verify that the Excel sheet has the expected structure"""
    try:
        # Load the Excel file
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)

        # Define the expected structure as a list of tuples containing ((row, col), expected_value)
        expected_structure = [
            # Section headers
            ((3, 0), "Feriendaten"),
            ((12, 0), "Ausserdem schulfrei"),
            ((17, 0), "Semesterdaten"),
            ((21, 0), "Jahresversammlung der Kantonalen Schulkonferenz"),
            # Column headers
            ((3, 1), "Beginn (Samstag)"),
            ((3, 2), "Ende (Sonntag)"),
            ((3, 3), "Schulanfang (Montag)"),
            ((12, 1), "Beginn"),
            ((12, 2), "Ende"),
            ((17, 1), "Beginn"),
            ((17, 2), "Ende"),
            # Holiday names
            ((4, 0), "Herbstferien"),
            ((5, 0), "Weihnachtsferien"),
            ((6, 0), "Fasnachts- und Sportferien"),
            ((7, 0), "Basler Fasnacht"),
            ((8, 0), "Frühjahrsferien"),
            ((9, 0), "Dreitageblock"),
            ((10, 0), "Sommerferien"),
            # Special days off
            ((13, 0), "1. Mai"),
            ((14, 0), "Auffahrt"),
            ((15, 0), "Pfingstmontag"),
            # Semester data
            ((18, 0), "1. Semester"),
            ((19, 0), "2. Semester"),
        ]

        # Sort by row, then by column
        expected_structure.sort(key=lambda x: (x[0][0], x[0][1]))

        # Check each expected value
        for (row, col), expected_value in expected_structure:
            actual_value = str(df.iloc[row, col])
            if expected_value != actual_value:
                position = get_excel_cell_reference(row, col)
                logging.error(
                    f"Sheet '{sheet_name}': Template verification failed: Expected '{expected_value}' at position {position}, but got '{actual_value}'"
                )
                return False

        # Skip date validation for template sheets
        if not is_template:
            # Check that critical date fields are not NaN
            date_coordinates = [
                (4, 1),
                (4, 2),
                (5, 1),
                (5, 2),
                (6, 1),
                (6, 2),
                (7, 1),
                (7, 2),
                (8, 1),
                (8, 2),
                (9, 1),
                (9, 2),
                (10, 1),
                (10, 2),
                (13, 1),
                (13, 2),
                (14, 1),
                (14, 2),
                (15, 1),
                (15, 2),
                (18, 1),
                (18, 2),
                (19, 1),
                (19, 2),
                (21, 1),
                (21, 2),
            ]
            for row, col in date_coordinates:
                if pd.isna(df.iloc[row, col]):
                    position = get_excel_cell_reference(row, col)
                    logging.error(
                        f"Sheet '{sheet_name}': Template verification failed: Date field at position {position} contains NaN value"
                    )
                    return False

                # Verify that the value is a valid date
                try:
                    date_value = df.iloc[row, col]
                    if not isinstance(date_value, (datetime.datetime, pd.Timestamp)):
                        # Try to convert to datetime - if it fails, it's not a valid date
                        converted = pd.to_datetime(date_value, errors="coerce")
                        if pd.isna(converted):
                            position = get_excel_cell_reference(row, col)
                            logging.error(
                                f"Sheet '{sheet_name}': Template verification failed: Field at position {position} contains '{date_value}' which is not a valid date"
                            )
                            return False
                except Exception as e:
                    position = get_excel_cell_reference(row, col)
                    logging.error(
                        f"Sheet '{sheet_name}': Template verification failed: Error validating date at position {position}: {str(e)}"
                    )
                    return False

        logging.info(f"Verification passed for sheet '{sheet_name}'!")
        return True

    except Exception as e:
        logging.error(f"Error verifying Excel sheet '{sheet_name}': {str(e)}")
        return False


def get_excel_cell_reference(row: int, col: int) -> str:
    """Convert zero-based row and column indices to Excel cell reference (A1, B2, etc.) or (row+1, col+1) format for columns beyond D"""
    if col == 0:
        return f"A{row + 1}"
    elif col == 1:
        return f"B{row + 1}"
    elif col == 2:
        return f"C{row + 1}"
    elif col == 3:
        return f"D{row + 1}"
    else:
        return f"({row + 1}, {col + 1})"


def get_smallest_year_from_excel(excel_path: str) -> int:
    """Get the smallest year from the Excel sheet names"""
    try:
        xl = pd.ExcelFile(excel_path)
        # Filter sheet names that are digits and convert to integers
        years = [int(sheet) for sheet in xl.sheet_names if sheet.isdigit()]
        return min(years) if years else None
    except Exception as e:
        logging.error(f"Error reading Excel file: {str(e)}")
        return None


def process_excel_file(excel_path: str, data_path_abs: str) -> None:
    """Process the Excel file and extract holiday data into a single CSV file with data from all year tabs"""
    xl = pd.ExcelFile(excel_path)

    # Filter sheets that are named with years (digits only)
    year_sheets = [sheet for sheet in xl.sheet_names if sheet.isdigit()]

    # Create a list to store all holiday data rows
    all_holiday_data = []

    for sheet in year_sheets:
        logging.info(f"Processing sheet {sheet}")
        year = int(sheet)

        # Verify the structure of the year sheet before processing
        if not verify_excel(excel_path, sheet):
            logging.error(f"Sheet {sheet} failed verification - cannot proceed")
            raise ValueError(f"Excel sheet {sheet} has an invalid structure")

        # Read the Excel sheet, skipping the first 4 rows (headers start at row 5)
        df = pd.read_excel(excel_path, sheet_name=sheet, header=3)

        # Process all rows in the sheet
        for i, row in df.iterrows():
            # Skip rows with empty names
            if pd.isna(row.iloc[0]):
                continue

            # Get event name and check if it's in our include list
            name = row.iloc[0]
            if name not in CSV_INCLUDE_EVENTS:
                continue

            # Process only rows with both start and end dates
            if not pd.isna(row.iloc[1]) and not pd.isna(row.iloc[2]):
                # Convert dates to datetime and then to string format
                start_date = pd.to_datetime(row.iloc[1]).strftime("%Y-%m-%d 00:00:00")
                end_date = pd.to_datetime(row.iloc[2]).strftime("%Y-%m-%d 23:59:00")

                # Get year from start date
                year = pd.to_datetime(row.iloc[1]).year

                # Add to our data collection
                all_holiday_data.append([year, name, start_date, end_date])

    # Create a single output CSV file
    with open(os.path.join(data_path_abs, csv_filename), "w", newline="", encoding="utf-8") as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=";")
        csv_writer.writerow(["year", "name", "start_date", "end_date"])

        # Write all rows to the CSV
        for row_data in all_holiday_data:
            csv_writer.writerow(row_data)


def push_data_csv_with_realtime_push(data_path_abs: str, filename: str):
    """Push a single CSV file to ODS with realtime push"""
    csv_file_path_abs = os.path.join(data_path_abs, filename)

    df = pd.read_csv(csv_file_path_abs, sep=";")
    df["year"] = df["year"].astype(str)

    logging.info(f"Uploading data of size {df.shape} with realtime push")
    common.ods_realtime_push_df(df, ODS_PUSH_URL)

    logging.info("Update data with realtime push successful!")


def update_ics_file_on_ftp_server() -> None:
    # Update create_ics module's data_dir to match our data_path_abs if in Docker environment
    if os.path.exists("/code/data_orig"):  # We're in Docker
        create_ics.data_dir = "/code/data"

    # Generate ICS file using create_ics.py
    logging.info("Generating ICS file...")
    try:
        ics_file_name = create_ics.main()
        logging.info(f"ICS file generation completed successfully: {ics_file_name}")

        # Upload the ICS file to FTP server
        if os.path.exists(ics_file_name):
            logging.info("Uploading ICS file to FTP server...")
            remote_path = "ed/schulferien"
            try:
                common.upload_ftp(
                    filename=ics_file_name,
                    remote_path=remote_path,
                )
                logging.info(f"ICS file uploaded successfully to FTP server in folder '{remote_path}'")
            except Exception as e:
                logging.error(f"Error uploading ICS file to FTP server: {str(e)}")
        else:
            logging.error(f"ICS file not found at path: {ics_file_name}")
    except Exception as e:
        logging.error(f"Error generating ICS file: {str(e)}")


def check_embargo(excel_path: str) -> bool:
    """
    Check if the embargo date has passed.
    Returns True if processing can proceed (embargo passed), False otherwise.
    """
    try:
        # Check if "Drucken" tab exists
        xl = pd.ExcelFile(excel_path)
        if "Drucken" not in xl.sheet_names:
            logging.error("Embargo check failed: 'Drucken' tab does not exist in the Excel file")
            return False

        # Read the embargo date from cell C2
        df = pd.read_excel(excel_path, sheet_name="Drucken", header=None)
        embargo_cell = df.iloc[1, 2]  # C2 is at index [1,2]

        # Check if the cell contains a valid date
        if not isinstance(embargo_cell, pd.Timestamp) and not isinstance(embargo_cell, datetime.datetime):
            logging.error(f"Embargo check failed: Cell C2 does not contain a valid date: {embargo_cell}")
            return False

        # Convert to datetime for comparison
        embargo_date = pd.to_datetime(embargo_cell).date()
        today = datetime.date.today()

        # Check if embargo date has passed
        if today <= embargo_date:
            logging.error(f"Embargo date not yet passed. Today: {today}, Embargo until: {embargo_date}")
            return False

        logging.info(f"Embargo date has passed. Today: {today}, Embargo was until: {embargo_date}")
        return True

    except Exception as e:
        logging.error(f"Error checking embargo date: {str(e)}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
