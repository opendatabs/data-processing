import logging
import os
import platform
import sqlite3
from shutil import copy2

import common
import dashboard_calc
import pandas as pd
from common import change_tracking as ct
from dotenv import load_dotenv

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_09")
FTP_PASS = os.getenv("FTP_PASS_09")


def _table_row_count(db_path, table):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(1) FROM {table}")
        return cur.fetchone()[0]
    except sqlite3.OperationalError:
        # table might not exist yet
        return 0
    finally:
        conn.close()


def parse_truncate(path, filename):
    path_to_orig_file = os.path.join(path, filename)
    path_to_copied_file = os.path.join("data", filename)
    logging.info(f"Copying file {path_to_orig_file} to {path_to_copied_file}...")
    copy2(path_to_orig_file, path_to_copied_file)
    # Parse, process, truncate and write csv file
    logging.info(f"Reading file {filename}...")
    data = pd.read_csv(
        path_to_copied_file,
        engine="python",
        sep=";",
        # encoding='ANSI',
        encoding="cp1252",
        dtype={
            "SiteCode": "category",
            "SiteName": "category",
            "DirectionName": "category",
            "LaneName": "category",
            "TrafficType": "category",
        },
    )
    logging.info(f"Processing {path_to_copied_file}...")
    data["DateTimeFrom"] = pd.to_datetime(data["Date"] + " " + data["TimeFrom"], format="%d.%m.%Y %H:%M")
    data["DateTimeTo"] = data["DateTimeFrom"] + pd.Timedelta(hours=1)
    data["Year"] = data["DateTimeFrom"].dt.year
    data["Month"] = data["DateTimeFrom"].dt.month
    data["Day"] = data["DateTimeFrom"].dt.day
    data["Weekday"] = data["DateTimeFrom"].dt.weekday
    data["HourFrom"] = data["DateTimeFrom"].dt.hour
    data["DayOfYear"] = data["DateTimeFrom"].dt.dayofyear

    # 'LSA_Count.csv'
    if "LSA" in filename:
        logging.info("Creating separate files for MIV and Velo...")
        data["Zst_id"] = data["SiteCode"]
        miv_data = data[data["TrafficType"] == "MIV"]
        velo_data = data[(data["TrafficType"] == "VV") | (data["TrafficType"] == "Velo")]
        fuss_data = data[(data["TrafficType"] == "FV") | (data["TrafficType"] == "Fussgänger")]
        miv_data["TrafficType"] = "MIV"
        velo_data["TrafficType"] = "Velo"
        fuss_data["TrafficType"] = "Fussgänger"
        miv_filename = "MIV_" + filename
        velo_filename = "Velo_" + filename
        fuss_filename = "Fussgaenger_" + filename
        miv_data.to_csv(os.path.join("data", miv_filename), sep=";", encoding="utf-8", index=False)
        velo_data.to_csv(os.path.join("data", velo_filename), sep=";", encoding="utf-8", index=False)
        fuss_data.to_csv(os.path.join("data", fuss_filename), sep=";", encoding="utf-8", index=False)
        logging.info("Creating files for dashboard for MIV and Velo data...")
        dashboard_calc.create_files_for_dashboard(miv_data, filename)
        dashboard_calc.create_files_for_dashboard(velo_data, filename)
        dashboard_calc.create_files_for_dashboard(fuss_data, filename)
        generated_filenames = generate_files(miv_data, miv_filename)
        generated_filenames += generate_files(velo_data, velo_filename)
        generated_filenames += generate_files(fuss_data, fuss_filename)
        # Add data to databases
        logging.info("Adding data to database MIV")
        conn = sqlite3.connect(os.path.join("data", "datasette", "MIV.db"))
        miv_data.to_sql("MIV", conn, if_exists="append", index=False)
        conn.commit()
        conn.close()
        logging.info("Adding data to database Velo_Fuss")
        conn = sqlite3.connect(os.path.join("data", "datasette", "Velo_Fuss.db"))
        velo_data.to_sql("Velo_Fuss", conn, if_exists="append", index=False)
        fuss_data.to_sql("Velo_Fuss", conn, if_exists="append", index=False)
        conn.commit()
        conn.close()
    # 'FLIR_KtBS_MIV6.csv', 'FLIR_KtBS_Velo.csv', 'FLIR_KtBS_FG.csv'
    elif "FLIR" in filename:
        logging.info("Retrieving Zst_id as the SiteCode...")
        data["Zst_id"] = data["SiteCode"]
        if "Fahrrad" in data.columns:
            data.drop(columns=["Fahrrad"], inplace=True)
        if "Fussgänger" in data.columns:
            data.drop(columns=["Fussgänger"], inplace=True)
        logging.info("Updating TrafficType depending on the filename for FLIR data...")
        data["TrafficType"] = "MIV" if "MIV6" in filename else "Velo" if "Velo" in filename else "Fussgänger"
        dashboard_calc.create_files_for_dashboard(data, filename)
        generated_filenames = generate_files(data, filename)
        if "MIV" in filename:
            logging.info("Adding data to database MIV")
            conn = sqlite3.connect(os.path.join("data", "datasette", "MIV.db"))
            data.to_sql("MIV", conn, if_exists="append", index=False)
            conn.commit()
            conn.close()
        else:
            logging.info("Adding data to database Velo_Fuss")
            conn = sqlite3.connect(os.path.join("data", "datasette", "Velo_Fuss.db"))
            data.to_sql("Velo_Fuss", conn, if_exists="append", index=False)
            conn.commit()
            conn.close()
    # 'MIV_Class_10_1.csv', 'Velo_Fuss_Count.csv', 'MIV_Speed.csv'
    else:
        logging.info("Retrieving Zst_id as the first word in SiteName...")
        data["Zst_id"] = data["SiteName"].str.split().str[0]
        # Set TrafficType based on filename
        if "Velo_Fuss_Count" in filename:
            # Convert VV to Velo annd FV to Fussgänger
            logging.info("Updating TrafficType for Velo_Fuss_Count data...")
            data["TrafficType"] = data["TrafficType"].replace({"VV": "Velo", "FV": "Fussgänger"})
        logging.info(f"Creating files for dashboard for the following data: {filename}...")
        dashboard_calc.create_files_for_dashboard(data, filename)
        generated_filenames = generate_files(data, filename)
        if "MIV_Class" in filename:
            logging.info("Adding data to database MIV")
            conn = sqlite3.connect(os.path.join("data", "datasette", "MIV.db"))
            data.to_sql("MIV", conn, if_exists="append", index=False)
            conn.commit()
            conn.close()
        if "Velo_Fuss_Count" in filename:
            logging.info("Adding data to database Velo_Fuss")
            conn = sqlite3.connect(os.path.join("data", "datasette", "Velo_Fuss.db"))
            data.to_sql("Velo_Fuss", conn, if_exists="append", index=False)
            conn.commit()
            conn.close()
        if "MIV_Speed" in filename:
            logging.info("Adding data to database MIV_Geschwindigkeitsklassen")
            conn = sqlite3.connect(os.path.join("data", "datasette", "MIV_Geschwindigkeitsklassen.db"))
            data.to_sql("MIV_Geschwindigkeitsklassen", conn, if_exists="append", index=False)
            conn.commit()
            conn.close()

    logging.info(f"Created the following files to further processing: {str(generated_filenames)}")
    return generated_filenames


def generate_files(df, filename):
    current_filename = os.path.join("data", "converted_" + filename)
    generated_filenames = []
    logging.info(f"Saving {current_filename}...")
    df.to_csv(current_filename, sep=";", encoding="utf-8", index=False)
    generated_filenames.append(current_filename)

    # Only keep latest n years of data
    keep_years = 2
    current_filename = os.path.join("data", "truncated_" + filename)
    logging.info(f"Creating dataset {current_filename}...")
    latest_year = df["Year"].max()
    years = range(latest_year - keep_years, latest_year + 1)
    logging.info(f"Keeping only data for the following years in the truncated file: {list(years)}...")
    truncated_data = df[df.Year.isin(years)]
    logging.info(f"Saving {current_filename}...")
    truncated_data.to_csv(current_filename, sep=";", encoding="utf-8", index=False)
    generated_filenames.append(current_filename)

    # Create a separate dataset per year
    all_years = df.Year.unique()
    for year in all_years:
        year_data = df[df.Year.eq(year)]
        current_filename = os.path.join("data", str(year) + "_" + filename)
        logging.info(f"Saving {current_filename}...")
        year_data.to_csv(current_filename, sep=";", encoding="utf-8", index=False)
        generated_filenames.append(current_filename)

    return generated_filenames


def create_databases():
    """
    Idempotently ensures the three SQLite databases and tables exist.
    Does NOT delete existing databases.
    """
    os.makedirs(os.path.join("data", "datasette"), exist_ok=True)

    logging.info("Ensuring MIV database & table exist...")
    conn = sqlite3.connect(os.path.join("data", "datasette", "MIV.db"))
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS MIV (
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
    """)
    conn.commit()
    conn.close()

    logging.info("Ensuring Velo_Fuss database & table exist...")
    conn = sqlite3.connect(os.path.join("data", "datasette", "Velo_Fuss.db"))
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Velo_Fuss (
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
    """)
    conn.commit()
    conn.close()

    logging.info("Ensuring MIV_Geschwindigkeitsklassen database & table exist...")
    conn = sqlite3.connect(os.path.join("data", "datasette", "MIV_Geschwindigkeitsklassen.db"))
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS MIV_Geschwindigkeitsklassen (
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
    """)
    conn.commit()
    conn.close()


def create_indices_databases():
    columns_to_index_miv = [
        "Zst_id",
        "SiteCode",
        "SiteName",
        "DateTimeFrom",
        "DateTimeTo",
        "DirectionName",
        "LaneCode",
        "LaneName",
        "ValuesApproved",
        "ValuesEdited",
        "Year",
        "Month",
        "Day",
        "Weekday",
        "HourFrom",
        "Date",
        "TimeFrom",
        "TimeTo",
        "DayOfYear",
    ]
    columns_to_index_velo_fuss = [
        "Zst_id",
        "SiteCode",
        "SiteName",
        "DateTimeFrom",
        "DateTimeTo",
        "DirectionName",
        "LaneCode",
        "LaneName",
        "ValuesApproved",
        "ValuesEdited",
        "Year",
        "Month",
        "Day",
        "Weekday",
        "HourFrom",
        "Date",
        "TimeFrom",
        "TimeTo",
        "DayOfYear",
    ]

    conn = sqlite3.connect(os.path.join("data", "datasette", "MIV.db"))
    common.create_indices(conn, "MIV", columns_to_index_miv)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(os.path.join("data", "datasette", "Velo_Fuss.db"))
    common.create_indices(conn, "Velo_Fuss", columns_to_index_velo_fuss)
    conn.commit()
    conn.close()

    conn = sqlite3.connect(os.path.join("data", "datasette", "MIV_Geschwindigkeitsklassen.db"))
    common.create_indices(conn, "MIV_Geschwindigkeitsklassen", columns_to_index_miv)
    conn.commit()
    conn.close()


def main():
    dashboard_calc.download_weather_station_data()
    create_databases()

    # Determine if any target tables are empty (force initial load if so)
    miv_db = os.path.join("data", "datasette", "MIV.db")
    vf_db = os.path.join("data", "datasette", "Velo_Fuss.db")
    speed_db = os.path.join("data", "datasette", "MIV_Geschwindigkeitsklassen.db")

    miv_empty = _table_row_count(miv_db, "MIV") == 0
    vf_empty = _table_row_count(vf_db, "Velo_Fuss") == 0
    speed_empty = _table_row_count(speed_db, "MIV_Geschwindigkeitsklassen") == 0

    filename_orig = [
        "MIV_Class_10_1.csv",
        "Velo_Fuss_Count.csv",
        "MIV_Speed.csv",
        "LSA_Count.csv",
        "FLIR_KtBS_MIV6.csv",
        "FLIR_KtBS_Velo.csv",
        "FLIR_KtBS_FG.csv",
    ]

    # Upload processed and truncated data
    for datafile in filename_orig:
        datafile_with_path = os.path.join("data_orig", datafile)

        targets_miv = (
            ("MIV_Class" in datafile) or ("MIV_Speed" in datafile) or ("LSA" in datafile) or ("MIV6" in datafile)
        )
        targets_vf = (
            ("Velo_Fuss_Count" in datafile) or ("LSA" in datafile) or ("Velo" in datafile) or ("FG" in datafile)
        )
        targets_speed = "MIV_Speed" in datafile

        initial_load_required = (
            (targets_miv and miv_empty) or (targets_vf and vf_empty) or (targets_speed and speed_empty)
        )

        if initial_load_required or ct.has_changed(datafile_with_path):
            file_names = parse_truncate("data_orig", datafile)
            for file in file_names:
                common.upload_ftp(file, FTP_SERVER, FTP_USER, FTP_PASS, "")
                os.remove(file)
            ct.update_hash_file(datafile_with_path)
        else:
            logging.info(f"Skip processing {datafile}: unchanged and target tables already populated.")

    dashboard_calc.upload_list_of_lists()

    # Upload original unprocessed data
    for orig_file in filename_orig:
        path_to_file = os.path.join("data", orig_file)
        if ct.has_changed(path_to_file):
            common.upload_ftp(path_to_file, FTP_SERVER, FTP_USER, FTP_PASS, "")
            ct.update_hash_file(path_to_file)

    create_indices_databases()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    logging.info("Python running on the following architecture:")
    logging.info(f"{platform.architecture()}")
    main()
    logging.info("Job successful!")
