import datetime
import logging
import os
from collections import defaultdict

import common
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_03")
FTP_PASS = os.getenv("FTP_PASS_03")
ODS_PUSH_URL = os.getenv("ODS_PUSH_URL_100046")
ODS_PUSH_URL_TRUEBUNG = os.getenv("ODS_PUSH_URL_100323")
TRUEBUNG_REMOTE_PATH = "onlinedaten/truebung"
TRUEBUNG_ARCHIVE_PATH = f"{TRUEBUNG_REMOTE_PATH}/archiv_ods"
TZ = "Europe/Zurich"


def localize_startzeitpunkt(series: pd.Series) -> pd.Series:
    """Localize naive RUES timestamps to Europe/Zurich, handling DST transitions."""
    dt = pd.to_datetime(series, format="%d.%m.%Y %H:%M:%S")
    try:
        return dt.dt.tz_localize(TZ, ambiguous="infer", nonexistent="shift_forward")
    except Exception:
        ambiguous = [False] * len(dt)
        for value in dt[dt.duplicated(keep=False)].unique():
            positions = dt.index[dt == value].tolist()
            ambiguous[positions[0]] = True  # first occurrence during fall-back = CEST
        return dt.dt.tz_localize(TZ, ambiguous=ambiguous, nonexistent="shift_forward")


def download_latest_data(truebung=False):
    local_path = os.path.join(os.path.dirname(__file__), "data_orig")
    if truebung:
        return common.download_ftp(
            [],
            FTP_SERVER,
            FTP_USER,
            FTP_PASS,
            TRUEBUNG_REMOTE_PATH,
            local_path,
            "*_RUES_Online_Truebung.csv",
            list_only=False,
        )
    return common.download_ftp(
        [],
        FTP_SERVER,
        FTP_USER,
        FTP_PASS,
        "onlinedaten",
        local_path,
        "*_RUES_Online_S3.csv",
        list_only=False,
    )


def push_data_files_old(csv_files, truebung=False):
    for file in csv_files:
        df = pd.read_csv(file["local_file"], sep=";")
        common.ods_realtime_push_df(df, url=ODS_PUSH_URL_TRUEBUNG if truebung else ODS_PUSH_URL)


def push_data_files(csv_files, truebung=False):
    # Dictionary to hold the files grouped by date
    dfs_by_date = defaultdict(pd.DataFrame)

    for file in csv_files:
        date_str = file["remote_file"][:10]  # Files are named like YYYY-MM-DD*.csv
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        dfs_by_date[date_obj.date()] = pd.concat(
            [dfs_by_date[date_obj.date()], pd.read_csv(file["local_file"], sep=";")]
        )

    for date, df in dfs_by_date.items():
        logging.info(f"Processing files for date {date}...")
        df = df.sort_values(by=["Startzeitpunkt"]).reset_index(drop=True)

        df["Startzeitpunkt"] = localize_startzeitpunkt(df["Startzeitpunkt"])
        df["Endezeitpunkt"] = df["Startzeitpunkt"] + datetime.timedelta(hours=1)
        df["Startzeitpunkt"] = df["Startzeitpunkt"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
        df["Endezeitpunkt"] = df["Endezeitpunkt"].dt.strftime("%Y-%m-%d %H:%M:%S%z")
        common.ods_realtime_push_df(df, url=ODS_PUSH_URL_TRUEBUNG if truebung else ODS_PUSH_URL)


def _list_truebung_archive_years():
    entries = common.download_ftp(
        [],
        FTP_SERVER,
        FTP_USER,
        FTP_PASS,
        TRUEBUNG_ARCHIVE_PATH,
        os.path.join(os.path.dirname(__file__), "data_orig"),
        "20[0-9][0-9]",
        list_only=True,
    )
    return sorted(entry["remote_file"] for entry in entries)


def download_archived_truebung(years=None):
    local_path = os.path.join(os.path.dirname(__file__), "data_orig")
    years = years or _list_truebung_archive_years()
    csv_files = []
    for year in years:
        csv_files.extend(
            common.download_ftp(
                [],
                FTP_SERVER,
                FTP_USER,
                FTP_PASS,
                f"{TRUEBUNG_ARCHIVE_PATH}/{year}",
                local_path,
                "*_RUES_Online_Truebung.csv",
            )
        )
    return csv_files


def push_archived_truebung(years=None):
    """Re-push all Trübung files from FTP archiv_ods (per year). Clear dataset 100323 in ODS first."""
    csv_files = download_archived_truebung(years=years)
    logging.info(f"Pushing {len(csv_files)} archived Trübung file(s) to ODS...")
    push_data_files(csv_files, truebung=True)


def archive_data_files(csv_files, truebung=False):
    archive_folder = "archiv_ods"
    for file in csv_files:
        # if yesterday or older, move to archive folder
        date_str = file["remote_file"][:10]
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        if date_obj.date() < datetime.date.today():
            from_name = f"{file['remote_path']}/{file['remote_file']}"
            if truebung:
                year = date_str[:4]
                common.ensure_ftp_dir(FTP_SERVER, FTP_USER, FTP_PASS, f"{TRUEBUNG_ARCHIVE_PATH}/{year}")
                to_name = f"{archive_folder}/{year}/{file['remote_file']}"
            else:
                to_name = f"roh/{archive_folder}/{file['remote_file']}"
            logging.info(f"Renaming file on FTP server from {from_name} to {to_name}...")
            common.rename_ftp(from_name, to_name, FTP_SERVER, FTP_USER, FTP_PASS)


def push_older_data_files():
    data_path = os.path.join(os.path.dirname(__file__), "data_orig")

    df1 = pd.read_csv(os.path.join(data_path, "online2002_2023.csv"), sep=",")
    # Transoform Startzeitpunkt and Endezeitpunkt to the format expected by ODS
    df1["Startzeitpunkt"] = pd.to_datetime(df1["Startzeitpunkt"], format="%Y-%m-%d %H:%M:%S").dt.strftime(
        "%d.%m.%Y %H:%M:%S"
    )
    df1["Endezeitpunkt"] = pd.to_datetime(df1["Endezeitpunkt"], format="%Y-%m-%d %H:%M:%S").dt.strftime(
        "%d.%m.%Y %H:%M:%S"
    )
    common.batched_ods_realtime_push(df1, url=ODS_PUSH_URL, chunk_size=25000)

    df3 = pd.read_csv(
        os.path.join(data_path, "Onliner_RUES_2023_1h_S3_OGD.csv"),
        sep=";",
        encoding="cp1252",
    )
    df3 = df3.rename(
        columns={
            "StartZeit": "Startzeitpunkt",
            "EndeZeit": "Endezeitpunkt",
            "Temp_S3 [°C]": "RUS.W.O.S3.TE",
            "pH_S3 [-]": "RUS.W.O.S3.PH",
            "O2_S3 [mg_O2/L]": "RUS.W.O.S3.O2",
            "LF_S3 [µS/cm_25°C]": "RUS.W.O.S3.LF",
        }
    )
    df3 = df3[
        [
            "Startzeitpunkt",
            "Endezeitpunkt",
            "RUS.W.O.S3.LF",
            "RUS.W.O.S3.O2",
            "RUS.W.O.S3.PH",
            "RUS.W.O.S3.TE",
        ]
    ]
    df3 = add_seconds(df3)
    df3 = df3.dropna(subset=["Startzeitpunkt", "Endezeitpunkt"])
    common.batched_ods_realtime_push(df3, url=ODS_PUSH_URL, chunk_size=25000)
    pass


def push_data_files_corrected():
    data_path_2024 = os.path.join(os.path.dirname(__file__), "data_orig", "20250203_Export_Bafu_2024.csv")

    df = pd.read_csv(data_path_2024, sep=";", encoding="cp1252")
    df = df.rename(
        columns={
            "von": "Startzeitpunkt",
            "bis": "Endezeitpunkt",
            "Temp_S3 [°C]": "RUS.W.O.S3.TE",
            "pH_S3 [-]": "RUS.W.O.S3.PH",
            "O2_S3 [mg_O2/L]": "RUS.W.O.S3.O2",
            "LF_S3 [µS/cm_25°C]": "RUS.W.O.S3.LF",
        }
    )
    df = add_seconds(df)
    common.batched_ods_realtime_push(df, url=ODS_PUSH_URL, chunk_size=25000)
    pass


def add_seconds(df):
    df.Startzeitpunkt = df.Startzeitpunkt + ":00"
    df.Endezeitpunkt = df.Endezeitpunkt + ":00"
    return df


def main():
    # Uncomment to parse, transform and push older files (corrected etc.)
    # push_older_data_files()
    # push_data_files_corrected()
    # push_archived_truebung()

    csv_files = download_latest_data()
    push_data_files_old(csv_files)
    archive_data_files(csv_files)

    csv_files_trueb = download_latest_data(truebung=True)
    push_data_files(csv_files_trueb, truebung=True)
    archive_data_files(csv_files_trueb, truebung=True)

    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
