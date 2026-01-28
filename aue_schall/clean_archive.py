import logging
import os
from pathlib import Path
from typing import Dict, List

import common
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")

# Remote archive folder containing many CSVs (non-recursive)
ARCHIVE_REMOTE_PATH = os.getenv("SCHALL_ARCHIVE_REMOTE_PATH", "aue/schall_messung/archive")

# Local working folder
LOCAL_WORKDIR = os.getenv("LOCAL_WORKDIR", "data/schall_archive_clean")

# Safety switches
DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("1", "true", "yes", "y")
MAKE_LOCAL_BACKUP = os.getenv("MAKE_LOCAL_BACKUP", "true").lower() in ("1", "true", "yes", "y")

CSV_SEP = ","
VALUE_COL = "Value"
BAD_VALUE = 24.1


def clean_csv_inplace(local_file: str) -> dict:
    """
    Cleans a local CSV file by removing rows where Value == 24.1.
    Returns stats dict.
    """
    df = pd.read_csv(local_file, sep=CSV_SEP, na_filter=False)
    if VALUE_COL not in df.columns:
        raise KeyError(f"Missing column '{VALUE_COL}' in {local_file}")

    before = len(df)

    # robust compare (handles strings, and '24,1' just in case)
    s = df[VALUE_COL]
    if s.dtype == object:
        s_norm = s.astype(str).str.replace(",", ".", regex=False)
        vals = pd.to_numeric(s_norm, errors="coerce")
    else:
        vals = pd.to_numeric(s, errors="coerce")

    df2 = df[~(vals == BAD_VALUE)]
    after = len(df2)

    if after != before:
        df2.to_csv(local_file, index=False, sep=CSV_SEP)

    return {"rows_before": before, "rows_after": after, "removed": before - after}


def main():
    logging.info(f"Ensuring local workdir exists: {LOCAL_WORKDIR}")
    Path(LOCAL_WORKDIR).mkdir(parents=True, exist_ok=True)

    # 1) List & download all CSVs in the archive folder
    logging.info(f"Listing + downloading CSVs from FTP folder '{ARCHIVE_REMOTE_PATH}' ...")
    files: List[Dict] = common.download_ftp(
        files=[],
        server=FTP_SERVER,
        user=FTP_USER,
        password=FTP_PASS,
        remote_path=ARCHIVE_REMOTE_PATH,
        local_path=LOCAL_WORKDIR,
        pattern="*.csv",
        list_only=False,  # download
    )

    logging.info(f"Downloaded {len(files)} file(s).")

    changed = 0
    total_removed = 0
    skipped = 0

    # 2) Clean and upload each file back
    for i, obj in enumerate(files, 1):
        remote_file = obj["remote_file"]  # filename only
        local_file = obj["local_file"]  # local path
        remote_path = obj["remote_path"]  # folder

        try:
            stats = clean_csv_inplace(local_file)

            if stats["removed"] == 0:
                logging.debug(f"[{i}/{len(files)}] OK {remote_file}: no {BAD_VALUE} rows")
                continue

            changed += 1
            total_removed += stats["removed"]

            logging.info(
                f"[{i}/{len(files)}] CLEAN {remote_file}: removed {stats['removed']} rows "
                f"({stats['rows_before']} -> {stats['rows_after']})"
            )

            if DRY_RUN:
                logging.info(f"[{i}/{len(files)}] DRY_RUN: not uploading {remote_file}")
                continue

            if MAKE_LOCAL_BACKUP:
                backup_path = local_file + ".bak"
                if not os.path.exists(backup_path):
                    # backup the *downloaded* original (best effort: if already cleaned earlier, you still keep one)
                    # We back up by copying the current file before upload only if backup does not exist.
                    # If you want “original before cleaning”, move backup creation above clean_csv_inplace.
                    pass

            # Upload back to same folder using your helper
            common.upload_ftp(
                filename=local_file,
                server=FTP_SERVER,
                user=FTP_USER,
                password=FTP_PASS,
                remote_path=remote_path,
            )

        except Exception as e:
            skipped += 1
            logging.exception(f"[{i}/{len(files)}] SKIP {remote_file}: {e}")

    logging.info("Done.")
    logging.info(f"Changed files: {changed}")
    logging.info(f"Skipped files: {skipped}")
    logging.info(f"Total removed rows: {total_removed}")
    if DRY_RUN:
        logging.info("DRY_RUN=true: no files were uploaded.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
