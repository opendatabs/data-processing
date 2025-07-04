import csv
import logging
import os
import pathlib
import shutil
import pandas as pd
import datetime
import common
import create_ics
from dotenv import load_dotenv

load_dotenv()

ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100397")

excel_file = "Schulferien BS.xlsx"

data_orig_path = "data_orig/"
data_path = "data/"


def verify_excel(excel_path: str, sheet_name: str) -> bool:
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
            #((21, 0), "Gesamtkonferenz KSBS:"),
            
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
            ((13, 0), "Schulfrei (1. Mai)"),
            ((14, 0), "Schulfrei (Auffahrt)"),
            ((15, 0), "Schulfrei (Pfingstmontag)"),
            
            # Semester data
            #((18, 0), "1. Semester"),
            #((19, 0), "2. Semester")
        ]

        # Sort by row, then by column
        expected_structure.sort(key=lambda x: (x[0][0], x[0][1]))
        
        # Check each expected value
        for (row, col), expected_value in expected_structure:
            actual_value = str(df.iloc[row, col])
            if expected_value != actual_value:
                logging.error(f"Template verification failed: Expected '{expected_value}' at position ({row+1}, {col+1}), but got '{actual_value}'")
                return False
        
        logging.info(f"Verification passed for sheet '{sheet_name}'!")
        return True
        
    except Exception as e:
        logging.error(f"Error verifying Excel sheet '{sheet_name}': {str(e)}")
        return False


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


def process_excel_file(excel_path: str, data_path_abs: str) -> list:
    """Process the Excel file and extract holiday data into separate CSV files per year tab"""
    xl = pd.ExcelFile(excel_path)
    
    # Filter sheets that are named with years (digits only)
    year_sheets = [sheet for sheet in xl.sheet_names if sheet.isdigit()]
    processed_files = []
    
    for sheet in year_sheets:
        logging.info(f"Processing sheet {sheet}")
        year = int(sheet)
        output_filename_csv = f"school_holidays_{year}.csv"
        
        # Verify the structure of the year sheet before processing
        if not verify_excel(excel_path, sheet):
            logging.error(f"Sheet {sheet} failed verification - cannot proceed")
            raise ValueError(f"Excel sheet {sheet} has an invalid structure")
        
        with open(os.path.join(data_path_abs, output_filename_csv), "w", newline="", encoding="utf-8") as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=";")
            csv_writer.writerow(["year", "name", "start_date", "end_date"])
            
            # Read the Excel sheet, skipping the first 4 rows (headers start at row 5)
            df = pd.read_excel(excel_path, sheet_name=sheet, header=3)
            
            # Find the row where "Feriendaten" or holiday data starts
            for i, row in df.iterrows():
                # Skip rows without a name or with specific excluded rows
                if pd.isna(row.iloc[0]) or row.iloc[0] in ["Ausserdem schulfrei", "Semesterdaten", "Gesamtkonferenz KSBS:"]:
                    continue
                    
                # Skip italicized entries (like "Basler Fasnacht" and "Dreitageblock")
                if row.iloc[0].startswith("Basler Fasnacht") or row.iloc[0].startswith("Dreitageblock"):
                    continue
                    
                # Process only rows with dates
                if not pd.isna(row.iloc[1]) and not pd.isna(row.iloc[2]):
                    name = row.iloc[0].strip()
                    
                    # Convert dates to datetime and then to string format
                    start_date = pd.to_datetime(row.iloc[1]).strftime("%Y-%m-%d 00:00:00")
                    end_date = pd.to_datetime(row.iloc[2]).strftime("%Y-%m-%d 23:59:00")
                    
                    # Get year from start date
                    year = pd.to_datetime(row.iloc[1]).year
                    
                    csv_writer.writerow([year, name, start_date, end_date])
            
            # Now process the "Ausserdem schulfrei" section if it exists
            found_schulfrei = False
            for i, row in df.iterrows():
                if not pd.isna(row.iloc[0]) and row.iloc[0] == "Ausserdem schulfrei":
                    found_schulfrei = True
                    continue
                    
                if found_schulfrei and not pd.isna(row.iloc[0]) and not pd.isna(row.iloc[1]):
                    # Skip if we've reached another section
                    if row.iloc[0] in ["Semesterdaten", "Gesamtkonferenz KSBS:"]:
                        break
                        
                    name = row.iloc[0].strip()
                    
                    # For single-day events, both start and end are the same day
                    start_date = pd.to_datetime(row.iloc[1]).strftime("%Y-%m-%d 00:00:00")
                    
                    # If end date is provided, use it, otherwise use the start date
                    if not pd.isna(row.iloc[2]):
                        end_date = pd.to_datetime(row.iloc[2]).strftime("%Y-%m-%d 23:59:00")
                    else:
                        # Raise error instead of defaulting to start date
                        raise ValueError(f"Missing end date for '{name}' starting on {start_date}")
                    
                    # Get year from start date
                    year = pd.to_datetime(row.iloc[1]).year
                    
                    csv_writer.writerow([year, name, start_date, end_date])
        
        processed_files.append(output_filename_csv)
    
    return processed_files


def push_all_data_csv_with_realtime_push(data_path_abs: str):
    for filename in os.listdir(data_path_abs):
        if not filename.endswith(".csv"):
            logging.info(f"Ignoring {filename}; Not a csv file")
            continue
        if not filename.startswith("school_holidays_"):
            logging.info(f"Ignoring {filename}; Not of the form school_holidays_*.csv")
            continue

        csv_file_path_abs = os.path.join(data_path_abs, filename)

        df = pd.read_csv(csv_file_path_abs, sep=";")
        df["year"] = df["year"].astype(str)

        logging.info(f"Uploading data of size {df.shape} with realtime push")
        common.ods_realtime_push_df(df, ODS_PUSH_URL)

    logging.info("Update data with realtime push successful!")


def clean_data_orig_folder(data_orig_path_abs) -> None:
    for item in os.listdir(data_orig_path_abs):
        item_path = os.path.join(data_orig_path_abs, item)
        if item != "dummy.txt":
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)

    logging.info(f'All files and folders in "{data_orig_path_abs}" except dummy.txt have been deleted.')


def update_ics_file_on_ftp_server() -> None:
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
        
        # Read the embargo date from cell B2
        df = pd.read_excel(excel_path, sheet_name="Drucken", header=None)
        embargo_cell = df.iloc[1, 1]  # B2 is at index [1,1]
        
        # Check if the cell contains a valid date
        if not isinstance(embargo_cell, pd.Timestamp) and not isinstance(embargo_cell, datetime.datetime):
            logging.error(f"Embargo check failed: Cell B2 does not contain a valid date: {embargo_cell}")
            return False
            
        # Convert to datetime for comparison
        embargo_date = pd.to_datetime(embargo_cell).date()
        today = datetime.date.today()
        
        # Check if embargo has passed
        if today <= embargo_date:
            logging.error(f"Embargo date not yet passed. Today: {today}, Embargo until: {embargo_date}")
            return False
            
        logging.info(f"Embargo date has passed. Today: {today}, Embargo was until: {embargo_date}")
        return True
        
    except Exception as e:
        logging.error(f"Error checking embargo date: {str(e)}")
        return False


def main():
    # TODO: This is really ugly, but the easiest way I found to make it runnable both locally and on the server
    script_dir = pathlib.Path(__file__).parent.absolute()
    if os.path.basename(script_dir) != "ed_schulferien":
        script_dir = os.path.join(script_dir, "ed_schulferien")

    data_path_abs = os.path.join(script_dir, data_path)
    data_orig_path_abs = os.path.join(script_dir, data_orig_path)
    excel_path = os.path.join(script_dir, excel_file)

    # Ensure directories exist
    os.makedirs(data_path_abs, exist_ok=True)
    os.makedirs(data_orig_path_abs, exist_ok=True)
    
    # Check embargo before proceeding
    if not check_embargo(excel_path):
        logging.warning("Processing aborted due to active embargo")
        return

    # Verify the Excel TEMPLATE sheet
    if not verify_excel(excel_path, "TEMPLATE"):
        raise ValueError("Excel TEMPLATE verification failed. Please check the template structure.")

    # Process the Excel file and create separate CSV files per year tab
    processed_files = process_excel_file(
        excel_path=excel_path,
        data_path_abs=data_path_abs,
    )
    
    logging.info(f"Processed {len(processed_files)} year tabs into separate CSV files")

    DEBUG = True
    if not DEBUG:
        # Update FTP and ODSP for each processed file
        for csv_file in processed_files:
            common.update_ftp_and_odsp(
                path_export=os.path.join(data_path_abs, csv_file),
                folder_name="ed/schulferien",
                dataset_id="100397",
            )

        # Push data with realtime push
        push_all_data_csv_with_realtime_push(data_path_abs=data_path_abs)

        # Update ICS file on FTP server
        update_ics_file_on_ftp_server()

        # Clean data_orig folder
        clean_data_orig_folder(data_orig_path_abs=data_orig_path_abs)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
