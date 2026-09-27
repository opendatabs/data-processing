import glob
import logging
import os
import re
from datetime import datetime

import common
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_ORIG = "data_orig"
DATA_DIR = "data"
ODS_ID = "100549"
FTP_REMOTE_PATH = "wahlen_abstimmungen/wahlen/gericht/2026-09"
CSV_NAME_TEMPLATE = "100549_resultate_ersatzwahl_appellationsgericht_{date}.csv"

CANDIDATE_HEADER_RE = re.compile(r"^(?P<name>.+?)\s*\((?P<nr>\d+)\)\s*$")

# Vorlage labels: «Briefl. und elektr.» instead of SESAM's «briefl. Stimmende».
WAHLLOKAL_LABELS = {
    "Basel briefl. Stimmende": "Basel briefl. & elektr. Stimmende (Total)",
    "Basel brieflich Stimmende": "Basel briefl. & elektr. Stimmende (Total)",
    "Riehen briefl. Stimmende": "Riehen briefl. & elektr. Stimmende (Total)",
    "Riehen brieflich Stimmende": "Riehen briefl. & elektr. Stimmende (Total)",
    "Bettingen briefl. Stimmende": "Bettingen briefl. & elektr. Stimmende (Total)",
    "Bettingen brieflich Stimmende": "Bettingen briefl. & elektr. Stimmende (Total)",
}

WAHLLOKAL_TO_GEMEINDE = {
    "Bahnhof SBB": "Stadt Basel",
    "Rathaus": "Stadt Basel",
    "Kleinbasel": "Stadt Basel",
    "Basel briefl. & elektr. Stimmende (Total)": "Stadt Basel",
    "Total Basel": "Stadt Basel",
    "Riehen Gemeindehaus": "Gemeinde Riehen",
    "Riehen briefl. & elektr. Stimmende (Total)": "Gemeinde Riehen",
    "Total Riehen": "Gemeinde Riehen",
    "Bettingen Gemeindehaus": "Gemeinde Bettingen",
    "Bettingen briefl. & elektr. Stimmende (Total)": "Gemeinde Bettingen",
    "Total Bettingen": "Gemeinde Bettingen",
    "Total Kanton": "Kanton Basel-Stadt",
}

BRIEFLICH_CHANNELS = {
    "Basel briefl. & elektr. Stimmende (Total)",
    "Riehen briefl. & elektr. Stimmende (Total)",
    "Bettingen briefl. & elektr. Stimmende (Total)",
}

GEMEINDE_TOTALS = {
    "Total Basel": "Stadt Basel",
    "Total Riehen": "Gemeinde Riehen",
    "Total Bettingen": "Gemeinde Bettingen",
    "Total Kanton": "Kanton Basel-Stadt",
}

SKIP_WAHLLOKALE_PREFIXES = ("Stimmenanteil", "Absolutes Mehr", "Gewählt ist")

# Exact T0012MAKA.TXT header, including capitalization.
MAKA_COLUMNS = [
    "Wahlbezeichnung",
    "Amtsdauer",
    "Wahltermin",
    "Anzahl Sitze",
    "Präsident",
    "Wahlkreis-Nr.",
    "Wahlkreis-Code",
    "Bezeichnung Wahlkreis",
    "Stimmberechtigte Männer",
    "Stimmberechtigte Frauen",
    "Stimmberechtigte",
    "Stimmberechtigte Auslandschweizer",
    "Wahlzettel",
    "Briefliche Stimmabgaben",
    "Ungestempelte Wahlzettel",
    "Ungültige Wahlzettel",
    "Leere Wahlzettel",
    "Leere Stimmen",
    "Ungültige Stimmen",
    "Vereinzelte Stimmen",
    "Leere Stimmen Präsident",
    "Ungültige Stimmen Präsident",
    "Vereinzelte Stimmen Präsident",
    "Kandidaten-Nr",
    "Personen-ID",
    "Bisher",
    "Gewählt",
    "Name",
    "Vorname",
    "Geschlecht",
    "Jahrgang",
    "Anrede",
    "Beruf",
    "Heimatort",
    "Strasse",
    "PLZ",
    "Ort",
    "Stimmen",
    "Stimmen Präsident",
    "Gewählt Präsident",
    "Total gültige Wahlzettel",
    "Stimmbeteiligung",
    "Anteil brieflich Wählende",
    "Absolutes Mehr",
]

INT_COLUMNS = [
    "Anzahl Sitze",
    "Stimmberechtigte Männer",
    "Stimmberechtigte Frauen",
    "Stimmberechtigte",
    "Stimmberechtigte Auslandschweizer",
    "Wahlzettel",
    "Briefliche Stimmabgaben",
    "Ungestempelte Wahlzettel",
    "Ungültige Wahlzettel",
    "Leere Wahlzettel",
    "Leere Stimmen",
    "Ungültige Stimmen",
    "Vereinzelte Stimmen",
    "Leere Stimmen Präsident",
    "Ungültige Stimmen Präsident",
    "Vereinzelte Stimmen Präsident",
    "Jahrgang",
    "PLZ",
    "Stimmen",
    "Stimmen Präsident",
    "Total gültige Wahlzettel",
    "Absolutes Mehr",
]

EXCEL_TO_MAKA_NUMBERS = {
    "wahlzettel": "Wahlzettel",
    "leere_wahlzettel": "Leere Wahlzettel",
    "ungultige_wahlzettel": "Ungültige Wahlzettel",
    "total_gultige_wahlzettel": "Total gültige Wahlzettel",
    "vereinzelte_stimmen": "Vereinzelte Stimmen",
    "stimmen": "Stimmen",
}


def main():
    logging.info("Building Ersatzwahl resultate from Excel + MAKA files...")
    df = calculate_resultate()
    wahltermin = parse_date_for_filename(df["Wahltermin"].iloc[0])
    os.makedirs(DATA_DIR, exist_ok=True)
    export_path = os.path.join(DATA_DIR, CSV_NAME_TEMPLATE.format(date=wahltermin))
    logging.info(f"Writing {export_path} ({len(df)} rows)...")
    df.to_csv(export_path, index=False)
    logging.info(f"Uploading to FTP {FTP_REMOTE_PATH} and publishing ODS {ODS_ID}...")
    common.update_ftp_and_odsp(export_path, FTP_REMOTE_PATH, ODS_ID)


def calculate_resultate(data_orig=DATA_ORIG):
    df_maka = read_maka(data_orig)
    excel_path = find_excel_with_results(data_orig)
    logging.info(f"Using Excel results from {excel_path}")
    df_excel, meta = read_excel_dat1(excel_path)
    df = combine_excel_and_maka(df_excel, df_maka, meta)
    validate_totals(df_excel)
    return df


def parse_date_for_filename(value):
    text = str(value).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text.replace(".", "-")


def find_excel_with_results(data_orig=DATA_ORIG):
    """Prefer Schlussresultate when present, otherwise Zwischenresultate. Skip Vorlage if possible."""
    patterns = (
        "Schlussresultate_*AppG*.xlsx",
        "Schlussresultat_*AppG*.xlsx",
        "Zwischenresultate_*AppG*.xlsx",
        "Zwischenresultat_*AppG*.xlsx",
    )
    files = []
    for pattern in patterns:
        files.extend(
            path
            for path in glob.glob(os.path.join(data_orig, pattern))
            if not os.path.basename(path).startswith("~$")
        )
    # Preserve order but drop duplicates (same path matched by multiple patterns).
    files = list(dict.fromkeys(files))
    if not files:
        raise FileNotFoundError(
            f"No Schluss-/Zwischenresultate Excel file (*AppG*.xlsx) found in {data_orig}/"
        )

    scored = []
    for path in files:
        try:
            df, _ = read_excel_dat1(path)
            votes = pd.to_numeric(df["wahlzettel"], errors="coerce").fillna(0).sum()
        except Exception as exc:
            logging.warning(f"Could not parse {path}: {exc}")
            votes = -1
        name = os.path.basename(path).lower()
        is_schluss = name.startswith("schluss")
        is_vorlage = "vorlage" in name
        # Prefer Schlussresultate, then non-Vorlage, then more filled-in votes.
        scored.append((is_schluss, not is_vorlage, votes, path))
    scored.sort(reverse=True)
    best_schluss, _, best_votes, best_path = scored[0]
    if best_votes <= 0:
        logging.warning("Excel files appear to contain no vote counts yet; using %s anyway", best_path)
    elif best_schluss:
        logging.info("Using Schlussresultate file (takes precedence over Zwischenresultate)")
    return best_path


def read_maka(data_orig=DATA_ORIG):
    frames = []
    for name in ("D0012MAKA.TXT", "T0012MAKA.TXT"):
        path = os.path.join(data_orig, name)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing MAKA file: {path}")
        logging.info(f"Reading MAKA file {path}")
        df = pd.read_csv(path, sep="\t", encoding="utf-8-sig", dtype=str, index_col=False)
        df.columns = [str(col).replace("\ufeff", "").strip() for col in df.columns]
        df = df.loc[:, ~df.columns.str.startswith("Unnamed")]
        df = df.dropna(how="all")
        frames.append(df)
    df = pd.concat(frames, ignore_index=True)
    if "Absolutes Mehr" not in df.columns:
        df["Absolutes Mehr"] = pd.NA
    df["Kandidaten-Nr"] = df["Kandidaten-Nr"].astype(str).str.strip().str.zfill(2)
    return df


def read_excel_dat1(path):
    raw = pd.read_excel(path, sheet_name="DAT 1", header=None, engine="openpyxl")
    title = str(raw.iloc[2, 1]).strip() if pd.notna(raw.iloc[2, 1]) else ""
    resultats_typ = _first_non_null(raw.iloc[1, 9:]) or "Zwischenresultat"

    header = ["" if pd.isna(value) else str(value).strip() for value in raw.iloc[6].tolist()]
    candidate_cols = {}
    absolutes_mehr_col = None
    for idx, label in enumerate(header):
        match = CANDIDATE_HEADER_RE.match(label)
        if match:
            candidate_cols[idx] = {
                "kandidaten_nr": match.group("nr").zfill(2),
                "header_name": match.group("name").strip(),
            }
        elif label.replace("\n", " ").strip().lower() == "absolutes mehr":
            absolutes_mehr_col = idx

    if not candidate_cols:
        raise ValueError(f"No candidate columns found in {path} sheet DAT 1")

    records = []
    absolutes_mehr_value = pd.NA
    for _, row in raw.iloc[7:].iterrows():
        wahllokal_raw = row.iloc[1]
        if pd.isna(wahllokal_raw):
            continue
        wahllokal = normalize_wahllokal(str(wahllokal_raw).strip())
        if wahllokal == "" or wahllokal.startswith(SKIP_WAHLLOKALE_PREFIXES):
            # Dedicated «Absolutes Mehr:» label row (value usually in col 3 or 4).
            if str(wahllokal_raw).strip().lower().startswith("absolutes mehr"):
                for col_idx in (3, 4, 2, absolutes_mehr_col):
                    if col_idx is None:
                        continue
                    value = to_number(row.iloc[col_idx]) if col_idx < len(row) else pd.NA
                    if pd.notna(value):
                        absolutes_mehr_value = value
                        break
            continue
        if wahllokal not in WAHLLOKAL_TO_GEMEINDE:
            logging.debug(f"Skipping Excel row that is not a result line: {wahllokal}")
            continue

        base = {
            "wahllokal": wahllokal,
            "wahlzettel": to_number(row.iloc[3]),
            "leere_wahlzettel": to_number(row.iloc[4]),
            "ungultige_wahlzettel": to_number(row.iloc[5]),
            "total_gultige_wahlzettel": to_number(row.iloc[6]),
            "vereinzelte_stimmen": to_number(row.iloc[9]) if len(row) > 9 else pd.NA,
        }
        if absolutes_mehr_col is not None and absolutes_mehr_col < len(row):
            row_mehr = to_number(row.iloc[absolutes_mehr_col])
            if pd.notna(row_mehr):
                base["absolutes_mehr"] = row_mehr
                if wahllokal == "Total Kanton":
                    absolutes_mehr_value = row_mehr
        for col_idx, cand in candidate_cols.items():
            rec = dict(base)
            rec["kandidaten_nr"] = cand["kandidaten_nr"]
            rec["stimmen"] = to_number(row.iloc[col_idx]) if col_idx < len(row) else pd.NA
            records.append(rec)

    df = pd.DataFrame(records)
    meta = {
        "wahlbezeichnung_excel": title,
        "resultats_typ": str(resultats_typ).strip(),
        "absolutes_mehr": absolutes_mehr_value,
    }
    return df, meta


def normalize_wahllokal(name):
    return WAHLLOKAL_LABELS.get(name, name)


def to_number(value):
    if pd.isna(value) or value == "":
        return pd.NA
    if isinstance(value, str):
        text = value.strip().replace("'", "").replace(" ", "")
        if text == "":
            return pd.NA
        value = text
    try:
        number = float(value)
    except (TypeError, ValueError):
        return pd.NA
    if pd.isna(number):
        return pd.NA
    if number.is_integer():
        return int(number)
    return number


def format_percent(ratio):
    if pd.isna(ratio):
        return pd.NA
    return f"{float(ratio) * 100:.2f}%"


def briefliche_by_gemeinde(df_excel):
    """Briefliche Stimmabgaben = Wahlzettel on the brieflich/elektronisch channel rows."""
    brieflich = df_excel[df_excel["wahllokal"].isin(BRIEFLICH_CHANNELS)].copy()
    if brieflich.empty:
        return {}
    # One value per Gemeinde (same for every candidate row).
    brieflich = brieflich.drop_duplicates(subset=["wahllokal"])
    brieflich["Bezeichnung Wahlkreis"] = brieflich["wahllokal"].map(WAHLLOKAL_TO_GEMEINDE)
    by_gemeinde = {
        row["Bezeichnung Wahlkreis"]: to_number(row["wahlzettel"])
        for _, row in brieflich.iterrows()
    }
    gemeinde_keys = ["Stadt Basel", "Gemeinde Riehen", "Gemeinde Bettingen"]
    kanton_parts = [by_gemeinde[key] for key in gemeinde_keys if key in by_gemeinde and pd.notna(by_gemeinde[key])]
    if kanton_parts:
        by_gemeinde["Kanton Basel-Stadt"] = int(sum(kanton_parts))
    return by_gemeinde


def resolve_absolutes_mehr(df_excel, meta):
    """Prefer Excel Total-Kanton / label value; fall back to (Wahlzettel - Ungültige) // 2 + 1."""
    value = meta.get("absolutes_mehr", pd.NA)
    if pd.notna(value):
        return int(value)

    kanton = df_excel[df_excel["wahllokal"] == "Total Kanton"]
    if not kanton.empty and "absolutes_mehr" in kanton.columns:
        row_value = kanton["absolutes_mehr"].dropna()
        if not row_value.empty:
            return int(row_value.iloc[0])

    if not kanton.empty:
        wahlzettel = pd.to_numeric(kanton["wahlzettel"], errors="coerce").iloc[0]
        ungueltige = pd.to_numeric(kanton["ungultige_wahlzettel"], errors="coerce").fillna(0).iloc[0]
        if pd.notna(wahlzettel):
            # Matches SESAM / Excel: Absolutes Mehr = (Wahlzettel - Ungültige) // 2 + 1
            return int(wahlzettel - ungueltige) // 2 + 1
    return pd.NA


def _first_non_null(values):
    for value in values:
        if pd.notna(value) and str(value).strip() != "":
            return value
    return None


def combine_excel_and_maka(df_excel, df_maka, meta=None):
    """Gemeinde + Kanton rows in T0012MAKA schema; Excel supplies the live result numbers."""
    meta = meta or {}
    excel_totals = df_excel[df_excel["wahllokal"].isin(GEMEINDE_TOTALS)].copy()
    excel_totals["Bezeichnung Wahlkreis"] = excel_totals["wahllokal"].map(GEMEINDE_TOTALS)
    excel_totals["Kandidaten-Nr"] = excel_totals["kandidaten_nr"]

    maka = df_maka.copy()
    for col in MAKA_COLUMNS:
        if col not in maka.columns:
            maka[col] = pd.NA

    maka = maka.drop_duplicates(subset=["Bezeichnung Wahlkreis", "Kandidaten-Nr"], keep="last")
    merged = maka.merge(
        excel_totals[["Bezeichnung Wahlkreis", "Kandidaten-Nr", *EXCEL_TO_MAKA_NUMBERS.keys()]],
        on=["Bezeichnung Wahlkreis", "Kandidaten-Nr"],
        how="inner",
        suffixes=("_maka", ""),
    )
    if merged.empty:
        raise ValueError("Could not match Excel Gemeinde/Kanton totals to MAKA rows.")

    for excel_col, maka_col in EXCEL_TO_MAKA_NUMBERS.items():
        merged[maka_col] = merged[excel_col]

    briefliche_map = briefliche_by_gemeinde(df_excel)
    merged["Briefliche Stimmabgaben"] = merged["Bezeichnung Wahlkreis"].map(briefliche_map)

    stimmberechtigte = pd.to_numeric(merged["Stimmberechtigte"], errors="coerce")
    wahlzettel = pd.to_numeric(merged["Wahlzettel"], errors="coerce")
    briefliche = pd.to_numeric(merged["Briefliche Stimmabgaben"], errors="coerce")
    merged["Stimmbeteiligung"] = [
        format_percent(w / s) if pd.notna(w) and pd.notna(s) and s else pd.NA
        for w, s in zip(wahlzettel, stimmberechtigte)
    ]
    merged["Anteil brieflich Wählende"] = [
        format_percent(b / w) if pd.notna(b) and pd.notna(w) and w else pd.NA
        for b, w in zip(briefliche, wahlzettel)
    ]

    absolutes_mehr = resolve_absolutes_mehr(df_excel, meta)
    merged["Absolutes Mehr"] = pd.NA
    if pd.notna(absolutes_mehr):
        merged.loc[merged["Bezeichnung Wahlkreis"] == "Kanton Basel-Stadt", "Absolutes Mehr"] = absolutes_mehr
        # Outcome is decided on Kanton Stimmen vs Absolutes Mehr; mirror to all rows.
        kanton_stimmen = (
            merged.loc[merged["Bezeichnung Wahlkreis"] == "Kanton Basel-Stadt", ["Kandidaten-Nr", "Stimmen"]]
            .drop_duplicates("Kandidaten-Nr")
            .set_index("Kandidaten-Nr")["Stimmen"]
        )
        kanton_stimmen = pd.to_numeric(kanton_stimmen, errors="coerce")
        elected = {
            nr: ("Gewählt" if pd.notna(s) and s >= absolutes_mehr else "Nicht gewählt")
            for nr, s in kanton_stimmen.items()
        }
        merged["Gewählt"] = merged["Kandidaten-Nr"].map(elected).fillna("Nicht gewählt")

    df = merged[MAKA_COLUMNS].copy()
    gemeinde_order = ["Stadt Basel", "Gemeinde Bettingen", "Gemeinde Riehen", "Kanton Basel-Stadt"]
    df["Bezeichnung Wahlkreis"] = pd.Categorical(df["Bezeichnung Wahlkreis"], categories=gemeinde_order, ordered=True)
    df = df.sort_values(["Bezeichnung Wahlkreis", "Kandidaten-Nr"]).reset_index(drop=True)
    df["Bezeichnung Wahlkreis"] = df["Bezeichnung Wahlkreis"].astype(str)

    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def validate_totals(df_excel):
    """Excel Gemeinde totals must equal the sum of Wahllokale (3 Basel urne kept separate)."""
    for gemeinde, total_name in (
        ("Stadt Basel", "Total Basel"),
        ("Gemeinde Riehen", "Total Riehen"),
        ("Gemeinde Bettingen", "Total Bettingen"),
    ):
        group = df_excel[df_excel["wahllokal"].map(WAHLLOKAL_TO_GEMEINDE) == gemeinde]
        for kandidaten_nr, cand_group in group.groupby("kandidaten_nr"):
            total_row = cand_group[cand_group["wahllokal"] == total_name]
            parts = cand_group[~cand_group["wahllokal"].isin(GEMEINDE_TOTALS)]
            if total_row.empty or parts.empty:
                continue
            for col in ("wahlzettel", "stimmen", "total_gultige_wahlzettel", "vereinzelte_stimmen"):
                expected = pd.to_numeric(parts[col], errors="coerce").fillna(0).sum()
                actual = pd.to_numeric(total_row[col], errors="coerce").fillna(0).iloc[0]
                if expected != actual:
                    logging.warning(
                        "Total mismatch %s %s candidate %s: parts %s != %s %s",
                        gemeinde,
                        col,
                        kandidaten_nr,
                        expected,
                        total_name,
                        actual,
                    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful.")
