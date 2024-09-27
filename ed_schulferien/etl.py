import csv
import logging
import os
import pathlib
import shutil
import zipfile
from datetime import timedelta

import pandas as pd
import vobject
from bs4 import BeautifulSoup

import common
from ed_schulferien import credentials

website_to_fetch_from = "https://www.bs.ch/themen/bildung-und-kinderbetreuung/schulferien"

data_orig_path = 'data_orig/'
data_path = 'data/'

def get_smallest_year(data_orig_path_abs: str) -> int:
    years = []
    for foldername in os.listdir(data_orig_path_abs):
        parts = foldername.split()
        if len(parts) >= 2 and parts[0] == "Schulferien" and parts[1].isdigit() and len(parts[1]) == 4:
            years.append(int(parts[1]))
    return min(years) if years else None

def fetch_data_from_website(data_orig_path_abs: str) -> None:
    
    os.makedirs(data_orig_path_abs, exist_ok=True)

    response = common.requests_get(website_to_fetch_from)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    zip_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]

    for link in zip_links:
        zip_filename = os.path.basename(link)
        zip_path = os.path.join(data_orig_path_abs, zip_filename)

        response = common.requests_get(link)
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_orig_path_abs)
        
        os.remove(zip_path)
    
    logging.info(f"Downloaded and extracted {len(zip_links)} zip files.")

def clean_name(name: str) -> str:
    name = name.strip()
    name_year = name.rsplit(" ", 1)
    try:
        int(name_year[1][:3])
        name = name_year[0]
    except ValueError:
        name = " ".join(name_year)
    except IndexError:
        name = name_year[0]
    if "1.Mai" in name:
        name = name.replace("1.Mai", "1. Mai")
    if "ü" in name:
        name = name.replace("ü", "ue")
    return name

def process_ics_file(file_path: str, csv_writer: csv.writer) -> None:
    with open(file_path, 'r', encoding='utf-8') as f:
        ics_content = f.read()
    
    cal = vobject.readOne(ics_content)
    
    for event in cal.vevent_list:
        name = clean_name(event.summary.value)
        
        start_date = event.dtstart.value

        end_date = event.dtend.value
        if end_date.weekday() != 6:
            end_date -= timedelta(days=1)
        
        year = start_date.year

        # Timestamps and 'if end_date.weekday() != 6' are added because of the ODS bug where a timestamp is introduced
        # when exporting automatically generated ics file. TODO: Can be simplified once this ods-bug is fixed
        start_date_str = start_date.strftime('%Y-%m-%d 00:00:00')
        end_date_str = end_date.strftime('%Y-%m-%d 23:59:00')

        csv_writer.writerow([year, name, start_date_str, end_date_str])

def transform_all_ics_to_csv(data_orig_path_abs: str, data_path_abs: str, output_filename_csv: str) -> None:
    
    csv_file_path = os.path.join(data_path_abs, output_filename_csv)
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=';')
        csv_writer.writerow(['year', 'name', 'start_date', 'end_date'])

        for foldername in os.listdir(data_orig_path_abs):
            if foldername == "dummy.txt":
                continue
            
            folder_path = os.path.join(data_orig_path_abs, foldername)
            if not os.path.isdir(folder_path):
                continue

            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)

                process_ics_file(file_path, csv_writer)

def push_all_data_csv_with_realtime_push(data_path_abs: str):

    for filename in os.listdir(data_path_abs):
        if not filename.endswith(".csv"):
            logging.info(f"Ignoring {filename}; Not a csv file")
            continue
        if not "school_holidays_since_" in filename:
            logging.info(f"Ignoring {filename}; Not of the form school_holidays_since_*.csv")
            continue

        csv_file_path_abs = os.path.join(data_path_abs, filename)

        df = pd.read_csv(csv_file_path_abs, sep=";")
        df["year"] = df["year"].astype(str)

        logging.info(f"Uploading data of size {df.shape} with realtime push")
        common.ods_realtime_push_df(df, url=credentials.ods_push_url, push_key=credentials.push_key)

    logging.info(f"Update data with realtime push successful!")

def clean_data_orig_folder(data_orig_path_abs) -> None:
    for item in os.listdir(data_orig_path_abs):
        item_path = os.path.join(data_orig_path_abs, item)
        if item != "dummy.txt":
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)

    logging.info(f'All files and folders in "{data_orig_path_abs}" except dummy.txt have been deleted.')

def main():
    # TODO: This is really ugly, but the easiest way I found to make it runnable both locally and on the server
    script_dir = pathlib.Path(__file__).parent.absolute()
    if os.path.basename(script_dir) != "ed_schulferien":
        script_dir = os.path.join(script_dir, "ed_schulferien")

    data_path_abs = os.path.join(script_dir, data_path)
    data_orig_path_abs = os.path.join(script_dir, data_orig_path)

    fetch_data_from_website(data_orig_path_abs=data_orig_path_abs)

    smallest_year = get_smallest_year(data_orig_path_abs)
    if smallest_year is None:
        raise ValueError("No valid year found in folder names")
    output_filename_csv = f'school_holidays_since_{smallest_year}.csv'

    transform_all_ics_to_csv(data_orig_path_abs=data_orig_path_abs,
                             data_path_abs=data_path_abs,
                             output_filename_csv=output_filename_csv)

    common.update_ftp_and_odsp(path_export=os.path.join(data_path_abs, output_filename_csv),
                               folder_name="ed/schulferien",
                               dataset_id="100397")

    push_all_data_csv_with_realtime_push(data_path_abs=data_path_abs)

    clean_data_orig_folder(data_orig_path_abs=data_orig_path_abs)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
