import logging
from pathlib import Path

import common
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_ORIG_DIR = Path("data_orig")
OUTPUT_DIR = Path("data")
ODS_DATASET_ID = "100542"
FTP_REMOTE_PATH = "dcc/datenkatalog"
OUTPUT_FILE = OUTPUT_DIR / f"{ODS_DATASET_ID}_datenkatalog_dienststellen_onboarding.csv"

SOURCE_COLUMNS = [
    "Departement",
    "Posten ",
    "Status: Kontaktiert",
    "Status: Info",
    "Status: Kick-Off",
    "Status: Metadatenerfassung",
    "Status: Review und Abnahme",
    "Status: Abgeschlossen",
]

OUTPUT_COLUMNS = [
    "Departement",
    "Posten",
    "Status: Kontaktiert",
    "Status: Info",
    "Status: Kick-Off",
    "Status: Metadatenerfassung",
    "Status: Review und Abnahme",
    "Status: Abgeschlossen",
]

STATUS_COLUMNS = OUTPUT_COLUMNS[2:]


def _find_source_file() -> Path:
    matches = sorted(DATA_ORIG_DIR.glob("*.xlsx"))
    if not matches:
        raise FileNotFoundError(f"No Excel file found in {DATA_ORIG_DIR}")
    if len(matches) > 1:
        raise ValueError(f"Expected one Excel file in {DATA_ORIG_DIR}, found {len(matches)}")
    return matches[0]


def _format_status_dates(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.copy()
    for column in STATUS_COLUMNS:
        formatted[column] = pd.to_datetime(formatted[column], errors="coerce").dt.strftime("%Y-%m-%d")
        formatted[column] = formatted[column].fillna("")
    return formatted


def _drop_rows_without_status(df: pd.DataFrame) -> pd.DataFrame:
    has_status = (
        df[STATUS_COLUMNS]
        .fillna("")
        .astype(str)
        .apply(lambda col: col.str.strip())
        .ne("")
        .any(axis=1)
    )
    return df[has_status].reset_index(drop=True)


def _extract_onboarding_df(source_path: Path) -> pd.DataFrame:
    source_df = pd.read_excel(source_path)
    missing = [column for column in SOURCE_COLUMNS if column not in source_df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in {source_path.name}: {missing}")

    df = source_df[SOURCE_COLUMNS].rename(columns={"Posten ": "Posten"})
    df["Posten"] = df["Posten"].str.removeprefix("Data Owner ")
    df = _format_status_dates(df)[OUTPUT_COLUMNS]
    return _drop_rows_without_status(df)


def main() -> None:
    """Extract onboarding status columns from the source Excel file."""
    logging.info("ETL job started")

    source_path = _find_source_file()
    logging.info("Reading source data from %s", source_path.name)
    df = _extract_onboarding_df(source_path)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    logging.info("Wrote %s (%d rows)", OUTPUT_FILE, len(df))

    common.update_ftp_and_odsp(str(OUTPUT_FILE), FTP_REMOTE_PATH, ODS_DATASET_ID)

    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Executing %s...", __file__)
    main()
    logging.info("Job successful.")
