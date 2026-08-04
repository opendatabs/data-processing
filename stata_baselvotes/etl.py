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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def parse_position_grosser_rat(text: str) -> dict[str, str | int]:
    """Parse Position des Grossen Rates into structured columns."""
    if pd.isna(text) or not text or not isinstance(text, str):
        return {
            "kategorie": "",
            "anzahl_ja": None,
            "anzahl_nein": None,
            "anzahl_ent": None,
            "stichfrage_gegenvorschlag_ja": None,
            "stichfrage_initiative_ja": None,
        }

    result = {
        "kategorie": "",
        "anzahl_ja": None,
        "anzahl_nein": None,
        "anzahl_ent": None,
        "stichfrage_gegenvorschlag_ja": None,
        "stichfrage_initiative_ja": None,
    }

    # Extract category
    if "Befürwortend" in text:
        result["kategorie"] = "Befürwortend"
    elif "Ablehnend" in text:
        result["kategorie"] = "Ablehnend"
    elif "Keine Empfehlung" in text or "keine Empfehlung" in text:
        result["kategorie"] = "Keine Empfehlung"

    # Extract Stichfrage information FIRST (before main vote counts to avoid conflicts)
    # Pattern: "bei Stichfrage mit 72 zu 15 für Gegenvorschlag"
    # 72 = votes FOR Gegenvorschlag, 15 = votes FOR Initiative (the other option)
    stichfrage_gegen_match = re.search(r"Stichfrage.*?(\d+).*?zu.*?(\d+).*?für\s+Gegenvorschlag", text)
    if stichfrage_gegen_match:
        result["stichfrage_gegenvorschlag_ja"] = int(stichfrage_gegen_match.group(1))
        result["stichfrage_initiative_ja"] = int(stichfrage_gegen_match.group(2))

    # Pattern: "bei Stichfrage 72 zu 15 Stimmen für Gegenvorschlag"
    stichfrage_gegen_match2 = re.search(r"Stichfrage\s+(\d+)\s+zu\s+(\d+)\s+Stimmen\s+für\s+Gegenvorschlag", text)
    if stichfrage_gegen_match2:
        result["stichfrage_gegenvorschlag_ja"] = int(stichfrage_gegen_match2.group(1))
        result["stichfrage_initiative_ja"] = int(stichfrage_gegen_match2.group(2))

    # Pattern for Initiative: "bei Stichfrage mit X zu Y für Initiative"
    stichfrage_init_match = re.search(r"Stichfrage.*?(\d+).*?zu.*?(\d+).*?für\s+Initiative", text)
    if stichfrage_init_match:
        result["stichfrage_initiative_ja"] = int(stichfrage_init_match.group(1))
        result["stichfrage_gegenvorschlag_ja"] = int(stichfrage_init_match.group(2))

    # Pattern: "bei Stichfrage X zu Y Stimmen für Initiative"
    stichfrage_init_match2 = re.search(r"Stichfrage\s+(\d+)\s+zu\s+(\d+)\s+Stimmen\s+für\s+Initiative", text)
    if stichfrage_init_match2:
        result["stichfrage_initiative_ja"] = int(stichfrage_init_match2.group(1))
        result["stichfrage_gegenvorschlag_ja"] = int(stichfrage_init_match2.group(2))

    # Extract main vote counts - try multiple patterns
    # Pattern 1: Standard format "79 Ja, 13 Nein" or "50 Ja, 41Nein" (missing space)
    ja_match = re.search(r"(\d+)\s+Ja", text)
    if ja_match:
        result["anzahl_ja"] = int(ja_match.group(1))

    nein_match = re.search(r"(\d+)\s*Nein", text)  # Allow optional space
    if nein_match:
        result["anzahl_nein"] = int(nein_match.group(1))

    # Pattern 2: "gegen" format "79 gegen 13 Stimmen" (only if not already found and not Stichfrage)
    if result["anzahl_ja"] is None and "gegen" in text and "Stichfrage" not in text:
        gegen_match = re.search(r"\((\d+)\s+gegen\s+(\d+)", text)
        if gegen_match:
            if result["kategorie"] == "Befürwortend":
                result["anzahl_ja"] = int(gegen_match.group(1))
                result["anzahl_nein"] = int(gegen_match.group(2))
            elif result["kategorie"] == "Ablehnend":
                result["anzahl_nein"] = int(gegen_match.group(1))
                result["anzahl_ja"] = int(gegen_match.group(2))

    # Pattern 3: "zu" format "79 zu 13 Stimmen" (only if not already found and not Stichfrage)
    if result["anzahl_ja"] is None and "zu" in text and "Stichfrage" not in text:
        zu_match = re.search(r"\((\d+)\s+zu\s+(\d+)", text)
        if zu_match:
            if result["kategorie"] == "Befürwortend":
                result["anzahl_ja"] = int(zu_match.group(1))
                result["anzahl_nein"] = int(zu_match.group(2))
            elif result["kategorie"] == "Ablehnend":
                result["anzahl_nein"] = int(zu_match.group(1))
                result["anzahl_ja"] = int(zu_match.group(2))

    # Pattern 4: "Grosses Mehr gegen X Stimmen" / "Grosse Mehrheit gegen X Stimmen"
    if result["anzahl_nein"] is None and (
        "Grosses Mehr gegen" in text or "Grosse Mehrheit gegen" in text or "grosse Mehrheit gegen" in text
    ):
        grosses_mehr_match = re.search(r"(?:Grosses Mehr|Grosse Mehrheit|grosse Mehrheit)\s+gegen\s+(\d+)", text)
        if grosses_mehr_match:
            gegen_count = int(grosses_mehr_match.group(1))
            if result["kategorie"] == "Befürwortend":
                result["anzahl_nein"] = gegen_count
            elif result["kategorie"] == "Ablehnend":
                result["anzahl_ja"] = gegen_count

    # Pattern 5: "alle gegen X"
    if result["anzahl_nein"] is None and "alle gegen" in text:
        alle_gegen_match = re.search(r"alle\s+gegen\s+(\d+)", text)
        if alle_gegen_match:
            gegen_count = int(alle_gegen_match.group(1))
            if result["kategorie"] == "Befürwortend":
                result["anzahl_nein"] = gegen_count
            elif result["kategorie"] == "Ablehnend":
                result["anzahl_ja"] = gegen_count

    # Pattern 6: "1 Gegenstimme" or "X Gegenstimmen"
    if result["anzahl_nein"] is None and "Gegenstimme" in text:
        gegenstimme_match = re.search(r"(\d+)\s+Gegenstimme", text)
        if gegenstimme_match:
            gegen_count = int(gegenstimme_match.group(1))
            if result["kategorie"] == "Befürwortend":
                result["anzahl_nein"] = gegen_count
            elif result["kategorie"] == "Ablehnend":
                result["anzahl_ja"] = gegen_count

    # Pattern 7: "einstimmig" - means 0 for the other side
    if "einstimmig" in text:
        if result["kategorie"] == "Befürwortend" and result["anzahl_nein"] is None:
            result["anzahl_nein"] = 0
        elif result["kategorie"] == "Ablehnend" and result["anzahl_ja"] is None:
            result["anzahl_ja"] = 0

    # Extract Enthaltungen (abstentions)
    ent_match = re.search(r"(\d+)\s+Ent", text)
    if ent_match:
        result["anzahl_ent"] = int(ent_match.group(1))

    return result


def parse_position_regierungsrat(text: str) -> str:
    """Parse Position des Regierungsrats into category."""
    if pd.isna(text) or not text or not isinstance(text, str):
        return ""

    text = text.strip()
    if "Befürwortend" in text:
        return "Befürwortend"
    elif "Ablehnend" in text:
        return "Ablehnend"
    elif "Keine Empfehlung" in text or "keine Empfehlung" in text:
        return "Keine Empfehlung"
    else:
        return text


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

    # Pattern: "Initiative: 11'311, Gegenvorschlag:29'213"
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

    # Parse Position des Grossen Rates
    logger.debug("Parsing Position des Grossen Rates column")
    position_gr_parsed = df["Position des Grossen Rates"].apply(parse_position_grosser_rat)
    df["Position des Grossen Rates Kategorie"] = position_gr_parsed.apply(lambda x: x["kategorie"])
    df["Position des Grossen Rates Ja"] = position_gr_parsed.apply(lambda x: x["anzahl_ja"])
    df["Position des Grossen Rates Nein"] = position_gr_parsed.apply(lambda x: x["anzahl_nein"])
    df["Position des Grossen Rates Enthaltung"] = position_gr_parsed.apply(lambda x: x["anzahl_ent"])
    df["Position des Grossen Rates Stichfrage Gegenvorschlag"] = position_gr_parsed.apply(
        lambda x: x["stichfrage_gegenvorschlag_ja"]
    )
    df["Position des Grossen Rates Stichfrage Initiative"] = position_gr_parsed.apply(
        lambda x: x["stichfrage_initiative_ja"]
    )

    # Parse Position des Regierungsrats
    logger.debug("Parsing Position des Regierungsrats column")
    df["Position des Regierungsrates Kategorie"] = df["Position des Regierungsrats"].apply(parse_position_regierungsrat)

    # Parse Stichfrage
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
