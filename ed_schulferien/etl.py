import os
import logging
import pathlib

import vobject
import csv
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import common
import zipfile


website_to_fetch_from = "https://www.bs.ch/themen/bildung-und-kinderbetreuung/schulferien"

data_orig_path = 'data_orig/'
data_path = 'data/'

output_filename_csv = 'school_holidays.csv'


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
        year = start_date.year

        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        csv_writer.writerow([year, name, start_date_str, end_date_str])

def transform_all_ics_to_csv(data_orig_path_abs: str, data_path_abs: str) -> None:
    
    os.makedirs(data_path_abs, exist_ok=True)

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

def main():
    script_dir = os.path.join(pathlib.Path(__file__).parents[1], "ed_schulferien")

    data_path_abs = os.path.join(script_dir, data_path)
    data_orig_path_abs = os.path.join(script_dir, data_orig_path)

    fetch_data_from_website(data_orig_path_abs=data_orig_path_abs)
    transform_all_ics_to_csv(data_orig_path_abs=data_orig_path_abs, data_path_abs=data_path_abs)
    
    common.update_ftp_and_odsp(path_export=os.path.join(data_path, output_filename_csv),
                        folder_name="ed/schulferien",
                        dataset_id="100397")
    
    

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
