import logging
from pathlib import Path
from typing import Any

import common
import common.change_tracking as ct
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

SOURCE_FILE = Path("data_orig") / "Tabelle_KlybeckDaten_Dashboard.xlsx"
OUTPUT_DIR = Path("data")

DUST_OUTPUT_FILE = OUTPUT_DIR / "100524_staubgebundene_schadstoffe_klybeck.csv"
VOLATILE_OUTPUT_FILE = OUTPUT_DIR / "100525_fluechtige_schadstoffe_klybeck.csv"
EXCEEDANCE_OUTPUT_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck.xlsx"
EXCEEDANCE_TRACKING_FILE = OUTPUT_DIR / "100526_gemessene_ueberschreitungen_klybeck_tracking.csv"

PASSIVE_PARAMS = {"Benzol", "∑CKW", "Naphthalin", "Naphtalin"}
ACTIVE_PARAMS = {"∑Aniline", "Nitrobenzol", "Phenol", "Methylphenole"}
DUST_PARAMS = {"PM10", "∑PAK", "Benzo(a)pyren"}

TARGET_COLUMNS = [
    "messbeginn",
    "messende",
    "standort",
    "parameter",
    "messwert",
    "interventionswert",
    "warnwert",
    "einheit",
    "messmethode",
]

EXCEEDANCE_COLUMNS = [
    "Messbeginn",
    "Messende",
    "Standort",
    "parameter",
    "messwert_ug_m3",
    "interventionswert_ug_m3",
    "Info / Massnahmen",
]


def _normalize_parameter(value: Any) -> str:
    parameter = str(value).strip()
    if parameter == "PM 10":
        return "PM10"
    if parameter == "Naphtalin":
        return "Naphthalin"
    return parameter


def _format_date(value: Any) -> str:
    if pd.isna(value):
        return ""
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value).strip()
    return ts.strftime("%Y-%m-%d")


def _format_number(value: Any, decimals: int | None = None) -> str:
    if pd.isna(value) or value == "":
        return ""
    number = float(value)
    if decimals is None:
        return f"{number:g}"
    return f"{number:.{decimals}f}"


def _to_float(value: Any) -> float | None:
    if pd.isna(value) or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _messmethode(parameter: str) -> str:
    if parameter in PASSIVE_PARAMS:
        return "VOC-Passivsammler"
    if parameter in ACTIVE_PARAMS:
        return "Aktivsammler"
    if parameter in DUST_PARAMS:
        return "Gravimetrie"
    return ""


def _build_exceedance_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    numeric = df.copy()
    numeric["messwert_num"] = numeric["messwert"].apply(_to_float)
    numeric["warnwert_num"] = numeric["warnwert"].apply(_to_float)
    numeric["interventionswert_num"] = numeric["interventionswert"].apply(_to_float)

    warn_exceedances = numeric[
        numeric["messwert_num"].notna()
        & numeric["warnwert_num"].notna()
        & (numeric["messwert_num"] >= numeric["warnwert_num"])
    ].copy()
    intervention_exceedances = numeric[
        numeric["messwert_num"].notna()
        & numeric["interventionswert_num"].notna()
        & (numeric["messwert_num"] >= numeric["interventionswert_num"])
    ].copy()

    return warn_exceedances, intervention_exceedances


def _build_excel_attachment(intervention_exceedances: pd.DataFrame) -> pd.DataFrame:
    attachment = intervention_exceedances[
        ["messbeginn", "messende", "standort", "parameter", "messwert", "interventionswert"]
    ].copy()
    attachment = attachment.rename(
        columns={
            "messbeginn": "Messbeginn",
            "messende": "Messende",
            "standort": "Standort",
            "messwert": "messwert_ug_m3",
            "interventionswert": "interventionswert_ug_m3",
        }
    )
    attachment["Info / Massnahmen"] = ""
    return attachment.reindex(columns=EXCEEDANCE_COLUMNS)


def _send_exceedance_email_if_changed(
    attachment_df: pd.DataFrame,
    warn_exceedances: pd.DataFrame,
    intervention_exceedances: pd.DataFrame,
) -> None:
    tracking_df = attachment_df.fillna("").sort_values(
        [
            "Messbeginn",
            "Messende",
            "Standort",
            "parameter",
            "messwert_ug_m3",
            "interventionswert_ug_m3",
        ]
    )
    tracking_df.to_csv(EXCEEDANCE_TRACKING_FILE, sep=";", index=False, encoding="utf-8")

    if not ct.has_changed(str(EXCEEDANCE_TRACKING_FILE)):
        logging.info("No change in exceedance content. Skipping workbook update and e-mail.")
        return

    attachment_df.to_excel(EXCEEDANCE_OUTPUT_FILE, index=False)
    text = "Das Klybeck Luftmessungs-ETL hat neue/veraenderte Ueberschreitungen erkannt.\n\n"
    text += f"Warnwert-Ueberschreitungen (>=): {len(warn_exceedances)}\n"
    text += f"Interventionswert-Ueberschreitungen (>=): {len(intervention_exceedances)}\n\n"
    text += "Im Anhang finden Sie die Datei mit Interventionswert-Ueberschreitungen.\n"
    text += "Spalte 'Info / Massnahmen' ist fuer manuelle Ergaenzungen vorgesehen.\n\n"
    text += "Kind regards,\nYour automated Open Data Basel-Stadt Python Job"

    msg = common.email_message(
        subject="Klybeck Luft: Ueberschreitungen Warnwert/Interventionswert",
        text=text,
        img=None,
        attachment=str(EXCEEDANCE_OUTPUT_FILE),
    )
    common.send_email(msg)
    ct.update_hash_file(str(EXCEEDANCE_TRACKING_FILE))
    logging.info("Sent exceedance e-mail with attachment %s", EXCEEDANCE_OUTPUT_FILE)


def _to_long_schema(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, str]] = []

    # First 4 rows contain metadata, following rows are measurement periods.
    for col_idx, col_name in enumerate(df.columns):
        if col_idx < 3:
            continue

        parameter = _normalize_parameter(df.iloc[0, col_idx])
        standort = str(col_name).split(".")[0].strip()
        interventionswert = _format_number(df.iloc[1, col_idx])
        warnwert = _format_number(df.iloc[2, col_idx])
        einheit = "" if pd.isna(df.iloc[3, col_idx]) else str(df.iloc[3, col_idx]).strip()
        messmethode = _messmethode(parameter)

        for row_idx in range(4, len(df)):
            messbeginn = _format_date(df.iloc[row_idx, 1])
            messende = _format_date(df.iloc[row_idx, 2])
            messwert = _format_number(df.iloc[row_idx, col_idx])

            record = {column: "" for column in TARGET_COLUMNS}
            record.update(
                {
                    "messbeginn": messbeginn,
                    "messende": messende,
                    "standort": standort,
                    "parameter": parameter,
                    "messwert": messwert,
                    "interventionswert": interventionswert,
                    "warnwert": warnwert,
                    "einheit": einheit,
                    "messmethode": messmethode,
                }
            )
            records.append(record)

    normalized = pd.DataFrame(records, columns=TARGET_COLUMNS)
    return normalized[
        ~(
            normalized["messbeginn"].eq("")
            & normalized["messende"].eq("")
            & normalized["messwert"].eq("")
        )
    ].reset_index(drop=True)


def main() -> None:
    """Create two Klybeck pollutant CSV files with the target schema."""
    logging.info("ETL job started")

    if not SOURCE_FILE.exists():
        raise FileNotFoundError(f"Source file not found: {SOURCE_FILE}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    source_df = pd.read_excel(SOURCE_FILE)
    long_df = _to_long_schema(source_df)
    warn_exceedances, intervention_exceedances = _build_exceedance_df(long_df)
    attachment_df = _build_excel_attachment(intervention_exceedances)

    volatile_params = PASSIVE_PARAMS.union(ACTIVE_PARAMS)
    volatile_df = long_df[long_df["parameter"].isin(volatile_params)].copy()
    dust_df = long_df[long_df["parameter"].isin(DUST_PARAMS)].copy()

    expected_volatile = {"Benzol", "∑CKW", "Naphthalin", "∑Aniline", "Nitrobenzol", "Phenol", "Methylphenole"}
    expected_dust = DUST_PARAMS

    missing_volatile = expected_volatile - set(volatile_df["parameter"].unique())
    missing_dust = expected_dust - set(dust_df["parameter"].unique())
    if missing_volatile:
        raise ValueError(f"Missing volatile parameters: {sorted(missing_volatile)}")
    if missing_dust:
        raise ValueError(f"Missing dust parameters: {sorted(missing_dust)}")
    if volatile_df.empty or dust_df.empty:
        raise ValueError("One or both output datasets are empty.")

    volatile_df.to_csv(VOLATILE_OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    logging.info("Wrote %s rows to %s", len(volatile_df), VOLATILE_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(VOLATILE_OUTPUT_FILE), "aue/luft/", "100525")
    dust_df.to_csv(DUST_OUTPUT_FILE, sep=";", index=False, encoding="utf-8")
    logging.info("Wrote %s rows to %s", len(dust_df), DUST_OUTPUT_FILE)
    common.update_ftp_and_odsp(str(DUST_OUTPUT_FILE), "aue/luft/", "100524")
    _send_exceedance_email_if_changed(attachment_df, warn_exceedances, intervention_exceedances)
    logging.info("ETL job completed")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")

