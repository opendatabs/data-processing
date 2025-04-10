import logging
from shutil import copy2
import pandas as pd
import common
from common import change_tracking as ct
from mobilitaet_verkehrszaehldaten.src import dashboard_calc
import sys
import os
import platform
import sqlite3

from dotenv import load_dotenv

load_dotenv()

PATH_ORIG = os.getenv("PATH_ORIG")
PATH_DEST = os.getenv("PATH_DEST")
FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")

def parse_truncate(path, filename, dest_path, no_file_cp):
    path_to_orig_file = os.path.join(path, filename)
    path_to_copied_file = os.path.join(dest_path, filename)
    if no_file_cp is False:
        logging.info(f"Copying file {path_to_orig_file} to {path_to_copied_file}...")
        copy2(path_to_orig_file, path_to_copied_file)
    # Parse, process, truncate and write csv file
    logging.info(f"Reading file {filename}...")
    data = pd.read_csv(path_to_copied_file,
                       engine='python',
                       sep=';',
                       # encoding='ANSI',
                       encoding='cp1252',
                       dtype={'SiteCode': 'category', 'SiteName': 'category', 'DirectionName': 'category',
                              'LaneName': 'category', 'TrafficType': 'category'})
    logging.info(f"Processing {path_to_copied_file}...")
    data['DateTimeFrom'] = pd.to_datetime(data['Date'] + ' ' + data['TimeFrom'], format='%d.%m.%Y %H:%M')
    data['DateTimeTo'] = data['DateTimeFrom'] + pd.Timedelta(hours=1)
    data['Year'] = data['DateTimeFrom'].dt.year
    data['Month'] = data['DateTimeFrom'].dt.month
    data['Day'] = data['DateTimeFrom'].dt.day
    data['Weekday'] = data['DateTimeFrom'].dt.weekday
    data['HourFrom'] = data['DateTimeFrom'].dt.hour
    data['DayOfYear'] = data['DateTimeFrom'].dt.dayofyear

    # 'LSA_Count.csv'
    if 'LSA' in filename:
        logging.info(f'Creating separate files for MIV and Velo...')
        data['Zst_id'] = data['SiteCode']
        miv_data = data[data['TrafficType'] == 'MIV']
        velo_data = data[(data['TrafficType'] != 'MIV') & (data['TrafficType'].notna())]
        miv_filename = 'MIV_' + filename
        velo_filename = 'Velo_' + filename
        miv_data.to_csv(os.path.join(dest_path, miv_filename), sep=';', encoding='utf-8', index=False)
        velo_data.to_csv(os.path.join(dest_path, velo_filename), sep=';', encoding='utf-8', index=False)
        dashboard_calc.create_files_for_dashboard(velo_data, filename, dest_path)
        dashboard_calc.create_files_for_dashboard(miv_data, filename, dest_path)
        generated_filenames = generate_files(miv_data, miv_filename, dest_path)
        generated_filenames += generate_files(velo_data, velo_filename, dest_path)
        # Add data to databases
        logging.info(f'Adding data to database MIV')
        conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV.db'))
        miv_data.to_sql('MIV', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
        logging.info(f'Adding data to database Velo_Fuss')
        conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'Velo_Fuss.db'))
        velo_data.to_sql('Velo_Fuss', conn, if_exists='append', index=False)
        conn.commit()
        conn.close()
    # 'FLIR_KtBS_MIV6.csv', 'FLIR_KtBS_Velo.csv', 'FLIR_KtBS_FG.csv'
    elif 'FLIR' in filename:
        logging.info(f'Retrieving Zst_id as the SiteCode...')
        data['Zst_id'] = data['SiteCode']
        if 'Fahrrad' in data.columns:
            data.drop(columns=['Fahrrad'], inplace=True)
        if 'Fussgänger' in data.columns:
            data.drop(columns=['Fussgänger'], inplace=True)
        logging.info(f'Updating TrafficType depending on the filename for FLIR data...')
        data['TrafficType'] = 'MIV' if 'MIV6' in filename else 'Velo' if 'Velo' in filename else 'Fussgänger'
        dashboard_calc.create_files_for_dashboard(data, filename, dest_path)
        generated_filenames = generate_files(data, filename, dest_path)
        if 'MIV' in filename:
            logging.info(f'Adding data to database MIV')
            conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV.db'))
            data.to_sql('MIV', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
        else:
            logging.info(f'Adding data to database Velo_Fuss')
            conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'Velo_Fuss.db'))
            data.to_sql('Velo_Fuss', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
    # 'MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv', 'MIV_Speed.csv'
    else:
        logging.info(f'Retrieving Zst_id as the first word in SiteName...')
        data['Zst_id'] = data['SiteName'].str.split().str[0]
        logging.info(f'Creating files for dashboard for the following data: {filename}...')
        dashboard_calc.create_files_for_dashboard(data, filename, dest_path)
        generated_filenames = generate_files(data, filename, dest_path)
        if 'MIV_Class' in filename:
            logging.info(f'Adding data to database MIV')
            conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV.db'))
            data.to_sql('MIV', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
        if 'Velo_Fuss_Count' in filename:
            logging.info(f'Adding data to database Velo_Fuss')
            conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'Velo_Fuss.db'))
            data.to_sql('Velo_Fuss', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()
        if 'MIV_Speed' in filename:
            logging.info(f'Adding data to database MIV_Geschwindigkeitsklassen')
            conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV_Geschwindigkeitsklassen.db'))
            data.to_sql('MIV_Geschwindigkeitsklassen', conn, if_exists='append', index=False)
            conn.commit()
            conn.close()

    logging.info(f'Created the following files to further processing: {str(generated_filenames)}')
    return generated_filenames


def generate_files(df, filename, dest_path):
    current_filename = os.path.join(dest_path, 'converted_' + filename)
    generated_filenames = []
    logging.info(f"Saving {current_filename}...")
    df.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    # Only keep latest n years of data
    keep_years = 2
    current_filename = os.path.join(dest_path, 'truncated_' + filename)
    logging.info(f'Creating dataset {current_filename}...')
    latest_year = df['Year'].max()
    years = range(latest_year - keep_years, latest_year + 1)
    logging.info(f'Keeping only data for the following years in the truncated file: {list(years)}...')
    truncated_data = df[df.Year.isin(years)]
    logging.info(f"Saving {current_filename}...")
    truncated_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
    generated_filenames.append(current_filename)

    # Create a separate dataset per year
    all_years = df.Year.unique()
    for year in all_years:
        year_data = df[df.Year.eq(year)]
        current_filename = os.path.join(dest_path, str(year) + '_' + filename)
        logging.info(f'Saving {current_filename}...')
        year_data.to_csv(current_filename, sep=';', encoding='utf-8', index=False)
        generated_filenames.append(current_filename)

    return generated_filenames


def create_databases():
    '''
    Creates three empty SQLite databases for the MIV, Velo_Fuss and MIV_Geschwindigkeitsklassen data.
    '''
    logging.info('Delete databases...')
    try:
        os.remove(os.path.join(PATH_DEST, 'datasette', 'MIV.db'))
        os.remove(os.path.join(PATH_DEST, 'datasette', 'Velo_Fuss.db'))
        os.remove(os.path.join(PATH_DEST, 'datasette', 'MIV_Geschwindigkeitsklassen.db'))
    except FileNotFoundError:
        pass
    logging.info('Creating databases...')
    logging.info('Creating MIV database...')
    conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV.db'))
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE MIV (
        Zst_id TEXT,
        SiteCode TEXT,
        SiteName TEXT, 
        DateTimeFrom TEXT, 
        DateTimeTo TEXT, 
        DirectionName TEXT, 
        LaneCode INT,
        LaneName TEXT, 
        ValuesApproved INT,
        ValuesEdited INT,
        TrafficType TEXT, 
        Total INT,
        MR INT,
        PW INT,
        'PW+' INT,
        Lief INT,
        'Lief+' INT,
        'Lief+Aufl.' INT,
        LW INT,
        'LW+' INT,
        Sattelzug INT,
        Bus INT,
        andere INT,
        Year INT, 
        Month INT, 
        Day INT, 
        Weekday INT, 
        HourFrom INT, 
        Date TEXT, 
        TimeFrom TEXT, 
        TimeTo TEXT, 
        DayOfYear INT
    )
    ''')
    conn.commit()
    conn.close()

    logging.info('Creating Velo_Fuss database...')
    conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'Velo_Fuss.db'))
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE Velo_Fuss (
        Zst_id TEXT,
        SiteCode TEXT,
        SiteName TEXT,
        DateTimeFrom TEXT,
        DateTimeTo TEXT,
        DirectionName TEXT,
        LaneCode INT,
        LaneName TEXT,
        ValuesApproved INT,
        ValuesEdited INT,
        TrafficType TEXT,
        Total INT,
        Year INT,
        Month INT,
        Day INT,
        Weekday INT,
        HourFrom INT,
        Date TEXT,
        TimeFrom TEXT,
        TimeTo TEXT,
        DayOfYear INT
    )
    ''')
    conn.commit()
    conn.close()

    logging.info('Creating MIV_Geschwindigkeitsklassen database...')
    conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV_Geschwindigkeitsklassen.db'))
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE MIV_Geschwindigkeitsklassen (
        Zst_id TEXT,
        SiteCode TEXT,
        SiteName TEXT,
        DateTimeFrom TEXT,
        DateTimeTo TEXT,
        DirectionName TEXT,
        LaneCode INT,
        LaneName TEXT,
        ValuesApproved INT,
        ValuesEdited INT,
        TrafficType TEXT,
        Total INT,
        '<20' INT,
        '20-30' INT,
        '30-40' INT,
        '40-50' INT,
        '50-60' INT,
        '60-70' INT,
        '70-80' INT,
        '80-90' INT,
        '90-100' INT,
        '100-110' INT,
        '110-120' INT,
        '120-130' INT,
        '>130' INT,
        Year INT,
        Month INT,
        Day INT,
        Weekday INT,
        HourFrom INT,
        Date TEXT,
        TimeFrom TEXT,
        TimeTo TEXT,
        DayOfYear INT
    )
    ''')
    conn.commit()
    conn.close()


def create_indices_databases():
    columns_to_index_miv = ['Zst_id', 'SiteCode', 'SiteName', 'DateTimeFrom', 'DateTimeTo', 'DirectionName', 'LaneCode',
                            'LaneName', 'ValuesApproved', 'ValuesEdited', 'Year', 'Month', 'Day', 'Weekday',
                            'HourFrom', 'Date', 'TimeFrom', 'TimeTo', 'DayOfYear']
    columns_to_index_velo_fuss = ['Zst_id', 'SiteCode', 'SiteName', 'DateTimeFrom', 'DateTimeTo', 'DirectionName',
                                  'LaneCode', 'LaneName', 'ValuesApproved', 'ValuesEdited', 'Year', 'Month', 'Day',
                                  'Weekday', 'HourFrom', 'Date', 'TimeFrom', 'TimeTo', 'DayOfYear']
    
    conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV.db'))
    common.create_indices(conn, 'MIV', columns_to_index_miv)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'Velo_Fuss.db'))
    common.create_indices(conn, 'Velo_Fuss', columns_to_index_velo_fuss)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(os.path.join(PATH_DEST, 'datasette', 'MIV_Geschwindigkeitsklassen.db'))
    common.create_indices(conn, 'MIV_Geschwindigkeitsklassen', columns_to_index_miv)
    conn.commit()
    conn.close()


def main():
    no_file_copy = False
    if 'no_file_copy' in sys.argv:
        no_file_copy = True
        logging.info('Proceeding without copying files...')

    dashboard_calc.download_weather_station_data(PATH_DEST)
    create_databases()

    filename_orig = ['MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv', 'MIV_Speed.csv',
                     'LSA_Count.csv',
                     'FLIR_KtBS_MIV6.csv', 'FLIR_KtBS_Velo.csv', 'FLIR_KtBS_FG.csv']

    # Upload processed and truncated data
    for datafile in filename_orig:
        datafile_with_path = os.path.join(PATH_ORIG, datafile)
        if ct.has_changed(datafile_with_path):
            file_names = parse_truncate(PATH_ORIG, datafile, PATH_DEST, no_file_copy)
            if not no_file_copy:
                for file in file_names:
                    common.upload_ftp(file, FTP_SERVER, FTP_USER, FTP_PASS, '')
                    os.remove(file)
            ct.update_hash_file(datafile_with_path)

    # Upload original unprocessed data
    if not no_file_copy:
        for orig_file in filename_orig:
            path_to_file = os.path.join(PATH_DEST, orig_file)
            if ct.has_changed(path_to_file):
                common.upload_ftp(path_to_file, FTP_SERVER, FTP_USER, FTP_PASS, '')
                ct.update_hash_file(path_to_file)
    
    create_indices_databases()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    logging.info(f'Python running on the following architecture:')
    logging.info(f'{platform.architecture()}')
    main()
    logging.info('Job successful!')
