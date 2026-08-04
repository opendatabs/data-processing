import logging
import re
from io import BytesIO
from pathlib import Path

import common
import pandas as pd
import requests

BASE_URL = "https://baselvotes.ch/abstimmungen/"
HEADERS = {"User-Agent": "stata-baselvotes-etl/1.0"}
SHEET_NAME = "Abstimmungen"
# Duplicate empty Abstimmungsergebnis becomes Abstimmungsergebnis.1 via pandas.
DROP_COLUMNS = ["Weiteres Bildmaterial", "Abstimmungsergebnis.1", "Result", "Beschreibung der Vorlag"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_stichfrage(text: str) -> dict[str, int | None]:
    """Parse Stichfrage column into Initiative and Gegenvorschlag counts."""
    if pd.isna(text) or not text or not isinstance(text, str):
        return {
            "initiative": None,
            "gegenvorschlag": None,
        }

    result = {
        "initiative": None,
        "gegenvorschlag": None,
    }

    # Pattern: "Initiative: 11'311, Gegenvorschlag: 29'213"
    init_match = re.search(r"Initiative:\s*([\d']+)", text)
    if init_match:
        init_value = init_match.group(1).replace("'", "")
        result["initiative"] = int(init_value)

    gegen_match = re.search(r"Gegenvorschlag:\s*([\d']+)", text)
    if gegen_match:
        gegen_value = gegen_match.group(1).replace("'", "")
        result["gegenvorschlag"] = int(gegen_value)

    return result


def add_parsed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add parsed columns derived from still-unstructured source fields."""
    original_col_count = len(df.columns)
    logger.info(f"Adding parsed columns to dataframe with {len(df)} rows and {original_col_count} columns")
    df = df.copy()

    logger.debug("Parsing Stichfrage column")
    stichfrage_parsed = df["Stichfrage"].apply(parse_stichfrage)
    df["Stichfrage Initiative"] = stichfrage_parsed.apply(lambda x: x["initiative"])
    df["Stichfrage Gegenvorschlag"] = stichfrage_parsed.apply(lambda x: x["gegenvorschlag"])

    new_col_count = len(df.columns) - original_col_count
    logger.info(f"Successfully added {new_col_count} new parsed columns (total: {len(df.columns)} columns)")
    return df


def export_csv(output_path: str | Path) -> None:
    output_path = Path(output_path)
    logger.info(f"Starting CSV export to {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Created output directory: {output_path.parent}")

    logger.info(f"Fetching data from {BASE_URL}")
    response = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    logger.debug(f"Received response with status code {response.status_code}")

    logger.debug("Extracting export form inputs (nonce and post IDs)")
    nonce = re.search(r'name="vote_export_nonce"\s+value="([^"]+)"', response.text)
    ids = re.search(r'name="exportpostids"\s+value="([^"]+)"', response.text)
    if not (nonce and ids):
        logger.error("Failed to find export form inputs on the page")
        raise RuntimeError("Export form inputs not found on the page.")
    logger.debug("Successfully extracted form inputs")

    logger.info("Requesting Excel export from server")
    xlsx_response = requests.post(
        BASE_URL,
        headers=HEADERS,
        data={
            "vote_export_nonce": nonce.group(1),
            "exportpostids": ids.group(1),
            "export_xls": "Export XLS",
        },
        timeout=30,
    )
    xlsx_response.raise_for_status()
    logger.info(f"Received Excel file ({len(xlsx_response.content)} bytes)")

    logger.info(f"Reading Excel sheet '{SHEET_NAME}' into DataFrame")
    df = pd.read_excel(BytesIO(xlsx_response.content), sheet_name=SHEET_NAME)
    logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from Excel")

    drop_cols = [c for c in DROP_COLUMNS if c in df.columns]
    if drop_cols:
        logger.info(f"Dropping columns: {drop_cols}")
        df = df.drop(columns=drop_cols)

    logger.info("Adding parsed columns")
    df = add_parsed_columns(df)

    logger.info(f"Writing CSV to {output_path}")
    df.to_csv(output_path, index=False)
    logger.info(f"Successfully exported {len(df)} rows and {len(df.columns)} columns to {output_path}")


def main() -> None:
    logger.info("Starting ETL process")
    try:
        path_export = "data/100518_baselvotes_abstimmungen.csv"
        export_csv(path_export)
        common.update_ftp_and_odsp(path_export, "/wahlen_abstimmungen/zeitreihe_volksabstimmungen", "100518")
        logger.info("Job successful!")
    except Exception as e:
        logger.exception(f"ETL process failed with error: {e}")
        raise


if __name__ == "__main__":
    main()
