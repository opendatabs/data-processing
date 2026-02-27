"""ETL for MeteoSchweiz daily weather data (station Basel-Binningen)."""

import logging
import os

import common
import common.change_tracking as ct
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_URL = "https://data.geo.admin.ch"
STATION = "bas"
SOURCES: dict[str, str] = {
    "smn": f"{BASE_URL}/ch.meteoschweiz.ogd-smn/{STATION}/ogd-smn_{STATION}_d",
    "nime": f"{BASE_URL}/ch.meteoschweiz.ogd-nime/{STATION}/ogd-nime_{STATION}_d",
    "obs": f"{BASE_URL}/ch.meteoschweiz.ogd-obs/{STATION}/ogd-obs_{STATION}_d",
}


def download_sources(period: str) -> dict[str, str]:
    """Download the three source CSVs for a given period.

    Args:
        period: Either "recent" or "historical".

    Returns:
        Dict mapping source name -> local file path.
    """
    raw_files: dict[str, str] = {}
    for name, base_url in SOURCES.items():
        url = f"{base_url}_{period}.csv"
        local_path = os.path.join("data_orig", f"raw_{name}_{period}.csv")
        logger.info("Downloading %s %s", name, period)
        resp = common.requests_get(url)
        with open(local_path, "wb") as f:
            f.write(resp.content)
        raw_files[name] = local_path
    return raw_files


def any_changed(raw_files: dict[str, str]) -> bool:
    """Check whether any of the three source files have changed.

    Args:
        raw_files: Dict mapping source name -> local file path.

    Returns:
        True if at least one file has changed.
    """
    changed = False
    for name, path in raw_files.items():
        if ct.has_changed(path):
            logger.info("%s has changed", name)
            changed = True
        else:
            logger.info("%s unchanged", name)
    return changed


def merge_sources(raw_files: dict[str, str]) -> pd.DataFrame:
    """Merge the three source CSVs on reference_timestamp.

    Overlapping non-key columns from nime are dropped in favour of smn.

    Args:
        raw_files: Dict mapping source name -> local file path.

    Returns:
        Merged DataFrame.
    """
    smn = pd.read_csv(raw_files["smn"], sep=";", low_memory=False)
    nime = pd.read_csv(raw_files["nime"], sep=";", low_memory=False)
    obs = pd.read_csv(raw_files["obs"], sep=";", low_memory=False)

    nime = nime.drop(columns=["station_abbr"])
    obs = obs.drop(columns=["station_abbr"])

    overlap_cols = [c for c in nime.columns if c in smn.columns and c != "reference_timestamp"]
    if overlap_cols:
        logger.info("Dropping overlapping columns from nime: %s", overlap_cols)
        nime = nime.drop(columns=overlap_cols)

    df = smn.merge(nime, on="reference_timestamp", how="outer")
    df = df.merge(obs, on="reference_timestamp", how="outer")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Derive date/jahr columns from reference_timestamp and reorder.

    Args:
        df: Merged DataFrame containing a reference_timestamp column.

    Returns:
        Transformed DataFrame with date and jahr columns up front
        and reference_timestamp removed.
    """
    ts = pd.to_datetime(df["reference_timestamp"], format="%d.%m.%Y %H:%M")
    df["date"] = ts.dt.strftime("%Y-%m-%d")
    df["jahr"] = ts.dt.strftime("%Y")
    df = df.drop(columns=["reference_timestamp"])
    cols = ["station_abbr", "date", "jahr"] + [c for c in df.columns if c not in ["station_abbr", "date", "jahr"]]
    return df[cols].sort_values("date").reset_index(drop=True)


def process_period(period: str, export_filename: str) -> None:
    """Download, check, merge, transform and export one period.

    Args:
        period: Either "recent" or "historical".
        export_filename: Name of the output CSV file in data/.
    """
    logger.info("Processing %s files", period)
    raw_files = download_sources(period)

    if not any_changed(raw_files):
        logger.info("No changes in %s sources, skipping", period)
        return

    logger.info("Changes detected in %s sources, merging", period)
    df = merge_sources(raw_files)
    df = transform(df)

    export_path = os.path.join("data", export_filename)
    df.to_csv(export_path, index=False, sep=";")
    logger.info("%s: %d rows -> %s", period, len(df), export_path)

    for path in raw_files.values():
        ct.update_hash_file(path)
    common.upload_ftp(export_path, remote_path="meteoschweiz/station_basel_binningen")
    common.publish_ods_dataset_by_id("100254")


def main() -> None:
    """Download, merge, transform and export MeteoSchweiz daily data."""
    process_period("recent", "100254_meteoschweiz_tageswerte_recent.csv")
    process_period("historical", "100254_meteoschweiz_tageswerte_historical.csv")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger.info("Executing %s", __file__)
    main()
    logger.info("Job successful")
