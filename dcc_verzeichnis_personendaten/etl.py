import csv
import io
import logging
import os
from typing import Any
from urllib.parse import urljoin

import common
import pandas as pd
from dataspot_auth import DataspotAuth

DATASPOT_BASE_URL = "https://datenkatalog.bs.ch"
DATASPOT_VVP_URL = (
    "https://datenkatalog.bs.ch/api/prod/schemes/7bbd63b2-48bc-4aeb-8e44-e950790b3ad0/download"
    "?mediaType=application%2Fx-xls&format=JSON&language=de"
)
ODS_DATASET_ID = "100520"
FTP_REMOTE_PATH = "dcc/verzeichnis_personendaten"
DATASPOT_TIMEZONE = "Europe/Zurich"

CSV_COLUMNS = [
    "departement",
    "abteilung",
    "bezeichnung",
    "rechtsgrundlage_n",
    "quelle_n",
    "verantwortliches_oeffentliches_organ",
    "internetauftritt",
    "zweck_der_datenbearbeitung",
    "stand",
    "source_url",
    "datenkatalog_url",
    "path",
]


def main() -> None:
    records = download_processing_records()
    rows = [map_processing_to_row(record) for record in records]

    if not rows:
        raise RuntimeError("No PUBLISHED Processing records found in Dataspot export.")

    df = pd.DataFrame(rows, columns=CSV_COLUMNS)
    os.makedirs("data", exist_ok=True)
    output_path = os.path.join("data", f"{ODS_DATASET_ID}_verzeichnis_personendaten.csv")
    df.to_csv(output_path, index=False, encoding="utf-8")
    logging.info("Wrote %d Processing records to %s.", len(df), output_path)

    common.update_ftp_and_odsp(output_path, FTP_REMOTE_PATH, ODS_DATASET_ID)
    logging.info("Published dataset %s from %s.", ODS_DATASET_ID, output_path)


def download_processing_records() -> list[dict[str, Any]]:
    auth = DataspotAuth()
    response = common.requests_get(url=DATASPOT_VVP_URL, headers=auth.get_headers())
    data = response.json()
    logging.info("Downloaded Dataspot scheme export from %s.", DATASPOT_VVP_URL)

    entries = normalize_export_entries(data)
    records = [entry for entry in entries if entry.get("_type") == "Processing" and entry.get("status") == "PUBLISHED"]
    logging.info("Filtered %d published Processing records from %d export entries.", len(records), len(entries))
    return records


def normalize_export_entries(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict) and isinstance(data.get("items"), list):
        entries = data["items"]
    elif isinstance(data, dict) and isinstance(data.get("results"), list):
        entries = data["results"]
    elif isinstance(data, dict) and isinstance(data.get("data"), list):
        entries = data["data"]
    else:
        raise ValueError("Unsupported Dataspot JSON export structure.")

    invalid_entries = [entry for entry in entries if not isinstance(entry, dict)]
    if invalid_entries:
        raise ValueError("Dataspot JSON export contains non-object entries.")

    return entries


def map_processing_to_row(processing: dict[str, Any]) -> dict[str, str]:
    departement, abteilung, verantwortliches_oeffentliches_organ, path = _parse_in_collection(
        _clean_value(processing.get("inCollection"))
    )

    return {
        "departement": departement,
        "abteilung": abteilung,
        "bezeichnung": _clean_value(processing.get("label")),
        "rechtsgrundlage_n": _clean_value(processing.get("legalFoundation")),
        "quelle_n": _clean_value(processing.get("legalFoundationSource")),
        "verantwortliches_oeffentliches_organ": verantwortliches_oeffentliches_organ,
        "internetauftritt": _clean_value(processing.get("website")),
        "zweck_der_datenbearbeitung": _clean_value(processing.get("dataProcessingPurpose")),
        "stand": _format_dataspot_date(processing.get("currentAsOf")),
        "source_url": "",
        "datenkatalog_url": _dataspot_url(_clean_value(processing.get("href"))),
        "path": path,
    }


def _parse_in_collection(in_collection: str) -> tuple[str, str, str, str]:
    if not in_collection:
        return "", "", "", ""

    reader = csv.reader(io.StringIO(in_collection), delimiter="/", quotechar='"')
    segments = [segment.strip() for segment in next(reader) if segment.strip()]
    departement = segments[1] if len(segments) > 1 else ""
    abteilung = segments[2] if len(segments) > 2 else ""
    verantwortliches_oeffentliches_organ = segments[-1] if segments else ""
    path = ">".join(segments)
    return departement, abteilung, verantwortliches_oeffentliches_organ, path


def _dataspot_url(href: str) -> str:
    if not href:
        return ""
    return urljoin(DATASPOT_BASE_URL, href)


def _clean_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _format_dataspot_date(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or not text.isdigit():
        return ""
    parsed = pd.to_datetime(int(text), unit="ms", utc=True).tz_convert(DATASPOT_TIMEZONE)
    return parsed.strftime("%Y-%m-%d")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Executing %s...", __file__)
    main()
    logging.info("Job successful.")
