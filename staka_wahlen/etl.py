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

PHYSICAL_URNE = {
    "Bahnhof SBB",
    "Rathaus",
    "Kleinbasel",
    "Riehen Gemeindehaus",
    "Bettingen Gemeindehaus",
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

SKIP_WAHLLOKALE_PREFIXES = ("Stimmenanteil",)

INT_COLUMNS = [
    "anzahl_sitze",
    "stimmberechtigte_manner",
    "stimmberechtigte_frauen",
    "stimmberechtigte",
    "stimmberechtigte_auslandschweizer",
    "stimmrechtsausweise",
    "wahlzettel",
    "briefliche_stimmabgaben",
    "ungestempelte_wahlzettel",
    "ungultige_wahlzettel",
    "leere_wahlzettel",
    "leere_stimmen",
    "ungultige_stimmen",
    "vereinzelte_stimmen",
    "stimmen",
    "stimmen_prasident",
    "total_gultige_wahlzettel",
    "absolutes_mehr",
    "jahrgang",
    "plz",
]

EXPORT_COLUMNS = [
    "wahlbezeichnung",
    "amtsdauer",
    "wahltermin",
    "anzahl_sitze",
    "resultats_typ",
    "wahllokal",
    "wahlkreis_nr",
    "wahlkreis_code",
    "bezeichnung_wahlkreis",
    "stimmberechtigte_manner",
    "stimmberechtigte_frauen",
    "stimmberechtigte",
    "stimmberechtigte_auslandschweizer",
    "stimmrechtsausweise",
    "wahlzettel",
    "briefliche_stimmabgaben",
    "ungestempelte_wahlzettel",
    "ungultige_wahlzettel",
    "leere_wahlzettel",
    "leere_stimmen",
    "ungultige_stimmen",
    "vereinzelte_stimmen",
    "kandidaten_nr",
    "personen_id",
    "bisher",
    "gewahlt",
    "ganzer_name",
    "name",
    "vorname",
    "geschlecht",
    "jahrgang",
    "anrede",
    "beruf",
    "heimatort",
    "strasse",
    "plz",
    "ort",
    "stimmen",
    "stimmen_prasident",
    "gewahlt_prasident",
    "total_gultige_wahlzettel",
    "stimmbeteiligung",
    "anteil_brieflich_wahlende",
    "absolutes_mehr",
]


def main():
    logging.info("Building Ersatzwahl resultate from Excel + MAKA files...")
    df = calculate_resultate()
    wahltermin = df["wahltermin"].iloc[0]
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
    validate_totals(df)
    return df


def find_excel_with_results(data_orig=DATA_ORIG):
    """Prefer the filled results file; fall back to Vorlage if that is the one with numbers."""
    files = [
        path
        for path in glob.glob(os.path.join(data_orig, "Zwischenresultate_*AppG*.xlsx"))
        if not os.path.basename(path).startswith("~$")
    ]
    if not files:
        raise FileNotFoundError(f"No Zwischenresultate Excel file found in {data_orig}/")

    scored = []
    for path in files:
        try:
            df, _ = read_excel_dat1(path)
            votes = pd.to_numeric(df["wahlzettel"], errors="coerce").fillna(0).sum()
        except Exception as exc:
            logging.warning(f"Could not parse {path}: {exc}")
            votes = -1
        is_vorlage = "vorlage" in os.path.basename(path).lower()
        scored.append((votes, not is_vorlage, path))
    scored.sort(reverse=True)
    best_votes, _, best_path = scored[0]
    if best_votes <= 0:
        logging.warning("Excel files appear to contain no vote counts yet; using %s anyway", best_path)
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
    df = df.rename(
        columns={
            "Wahlbezeichnung": "wahlbezeichnung",
            "Amtsdauer": "amtsdauer",
            "Wahltermin": "wahltermin_raw",
            "Anzahl Sitze": "anzahl_sitze",
            "Präsident": "prasident",
            "Wahlkreis-Nr.": "wahlkreis_nr",
            "Wahlkreis-Nr": "wahlkreis_nr",
            "Wahlkreis-Code": "wahlkreis_code",
            "Bezeichnung Wahlkreis": "bezeichnung_wahlkreis",
            "Stimmberechtigte Männer": "stimmberechtigte_manner",
            "Stimmberechtigte Frauen": "stimmberechtigte_frauen",
            "Stimmberechtigte": "stimmberechtigte",
            "Stimmberechtigte Auslandschweizer": "stimmberechtigte_auslandschweizer",
            "Wahlzettel": "wahlzettel",
            "Briefliche Stimmabgaben": "briefliche_stimmabgaben",
            "Ungestempelte Wahlzettel": "ungestempelte_wahlzettel",
            "Ungültige Wahlzettel": "ungultige_wahlzettel",
            "Leere Wahlzettel": "leere_wahlzettel",
            "Leere Stimmen": "leere_stimmen",
            "Ungültige Stimmen": "ungultige_stimmen",
            "Vereinzelte Stimmen": "vereinzelte_stimmen",
            "Leere Stimmen Präsident": "leere_stimmen_prasident",
            "Ungültige Stimmen Präsident": "ungultige_stimmen_prasident",
            "Vereinzelte Stimmen Präsident": "vereinzelte_stimmen_prasident",
            "Kandidaten-Nr": "kandidaten_nr",
            "Personen-ID": "personen_id",
            "Bisher": "bisher",
            "Gewählt": "gewahlt",
            "Name": "name",
            "Vorname": "vorname",
            "Geschlecht": "geschlecht",
            "Jahrgang": "jahrgang",
            "Anrede": "anrede",
            "Beruf": "beruf",
            "Heimatort": "heimatort",
            "Strasse": "strasse",
            "PLZ": "plz",
            "Ort": "ort",
            "Stimmen": "stimmen",
            "Stimmen Präsident": "stimmen_prasident",
            "Gewählt Präsident": "gewahlt_prasident",
            "Total gültige Wahlzettel": "total_gultige_wahlzettel",
            "Stimmbeteiligung": "stimmbeteiligung",
            "Anteil brieflich Wählende": "anteil_brieflich_wahlende",
            "Absolutes Mehr": "absolutes_mehr",
        }
    )
    df["kandidaten_nr"] = df["kandidaten_nr"].astype(str).str.strip().str.zfill(2)
    df["wahltermin"] = df["wahltermin_raw"].map(parse_wahltermin)
    df["ganzer_name"] = df["name"].str.strip() + ", " + df["vorname"].str.strip()
    return df


def parse_wahltermin(value):
    if pd.isna(value) or str(value).strip() == "":
        return pd.NA
    text = str(value).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text


def read_excel_dat1(path):
    raw = pd.read_excel(path, sheet_name="DAT 1", header=None, engine="openpyxl")
    title = str(raw.iloc[2, 1]).strip() if pd.notna(raw.iloc[2, 1]) else ""
    resultats_typ = _first_non_null(raw.iloc[1, 9:]) or "Zwischenresultat"

    header = ["" if pd.isna(value) else str(value).strip() for value in raw.iloc[6].tolist()]
    candidate_cols = {}
    for idx, label in enumerate(header):
        match = CANDIDATE_HEADER_RE.match(label)
        if match:
            candidate_cols[idx] = {
                "kandidaten_nr": match.group("nr").zfill(2),
                "header_name": match.group("name").strip(),
            }

    if not candidate_cols:
        raise ValueError(f"No candidate columns found in {path} sheet DAT 1")

    records = []
    for _, row in raw.iloc[7:].iterrows():
        wahllokal_raw = row.iloc[1]
        if pd.isna(wahllokal_raw):
            continue
        wahllokal = normalize_wahllokal(str(wahllokal_raw).strip())
        if wahllokal == "" or wahllokal.startswith(SKIP_WAHLLOKALE_PREFIXES):
            continue
        if wahllokal not in WAHLLOKAL_TO_GEMEINDE:
            logging.debug(f"Skipping Excel row that is not a result line: {wahllokal}")
            continue

        base = {
            "wahllokal": wahllokal,
            "stimmrechtsausweise": to_number(row.iloc[2]),
            "wahlzettel": to_number(row.iloc[3]),
            "leere_wahlzettel": to_number(row.iloc[4]),
            "ungultige_wahlzettel": to_number(row.iloc[5]),
            "total_gultige_wahlzettel": to_number(row.iloc[6]),
            "vereinzelte_stimmen": to_number(row.iloc[9]) if len(row) > 9 else pd.NA,
            "absolutes_mehr_excel": to_number(row.iloc[10]) if len(row) > 10 else pd.NA,
        }
        for col_idx, cand in candidate_cols.items():
            rec = dict(base)
            rec["kandidaten_nr"] = cand["kandidaten_nr"]
            rec["header_name"] = cand["header_name"]
            rec["stimmen"] = to_number(row.iloc[col_idx]) if col_idx < len(row) else pd.NA
            records.append(rec)

    df = pd.DataFrame(records)
    meta = {"wahlbezeichnung_excel": title, "resultats_typ": str(resultats_typ).strip()}
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


def _first_non_null(values):
    for value in values:
        if pd.notna(value) and str(value).strip() != "":
            return value
    return None


def combine_excel_and_maka(df_excel, df_maka, meta):
    maka_by_gemeinde = df_maka.drop_duplicates(subset=["bezeichnung_wahlkreis", "kandidaten_nr"])
    maka_candidates = df_maka.drop_duplicates(subset=["kandidaten_nr"]).set_index("kandidaten_nr")
    maka_kanton = df_maka[df_maka["bezeichnung_wahlkreis"] == "Kanton Basel-Stadt"]
    absolutes_mehr_kanton = pd.NA
    if "absolutes_mehr" in maka_kanton.columns and not maka_kanton.empty:
        absolutes_mehr_kanton = maka_kanton["absolutes_mehr"].dropna().iloc[0] if maka_kanton["absolutes_mehr"].notna().any() else pd.NA

    wahlbezeichnung = meta["wahlbezeichnung_excel"]
    if not wahlbezeichnung or wahlbezeichnung.endswith("vom"):
        wahlbezeichnung = maka_candidates["wahlbezeichnung"].iloc[0]

    rows = []
    for rec in df_excel.to_dict(orient="records"):
        wahllokal = rec["wahllokal"]
        gemeinde = WAHLLOKAL_TO_GEMEINDE[wahllokal]
        kandidaten_nr = rec["kandidaten_nr"]
        maka_row = maka_by_gemeinde[
            (maka_by_gemeinde["bezeichnung_wahlkreis"] == gemeinde) & (maka_by_gemeinde["kandidaten_nr"] == kandidaten_nr)
        ]
        if maka_row.empty:
            raise ValueError(f"No MAKA row for {gemeinde} / candidate {kandidaten_nr}")
        maka = maka_row.iloc[0]
        cand = maka_candidates.loc[kandidaten_nr]

        is_gemeinde_total = wahllokal in GEMEINDE_TOTALS
        is_kanton = wahllokal == "Total Kanton"
        briefliche = briefliche_for_wahllokal(wahllokal, rec, maka)

        out = {
            "wahlbezeichnung": wahlbezeichnung,
            "amtsdauer": cand["amtsdauer"],
            "wahltermin": cand["wahltermin"],
            "anzahl_sitze": cand["anzahl_sitze"],
            "resultats_typ": meta["resultats_typ"],
            "wahllokal": wahllokal,
            "wahlkreis_nr": maka["wahlkreis_nr"],
            "wahlkreis_code": maka["wahlkreis_code"],
            "bezeichnung_wahlkreis": gemeinde,
            "stimmberechtigte_manner": maka["stimmberechtigte_manner"] if is_gemeinde_total else pd.NA,
            "stimmberechtigte_frauen": maka["stimmberechtigte_frauen"] if is_gemeinde_total else pd.NA,
            "stimmberechtigte": maka["stimmberechtigte"] if is_gemeinde_total else pd.NA,
            "stimmberechtigte_auslandschweizer": maka["stimmberechtigte_auslandschweizer"] if is_gemeinde_total else pd.NA,
            "stimmrechtsausweise": rec["stimmrechtsausweise"],
            "wahlzettel": rec["wahlzettel"],
            "briefliche_stimmabgaben": briefliche,
            "ungestempelte_wahlzettel": maka["ungestempelte_wahlzettel"] if is_gemeinde_total else 0,
            "ungultige_wahlzettel": rec["ungultige_wahlzettel"],
            "leere_wahlzettel": rec["leere_wahlzettel"],
            "leere_stimmen": maka["leere_stimmen"] if is_gemeinde_total else 0,
            "ungultige_stimmen": maka["ungultige_stimmen"] if is_gemeinde_total else 0,
            "vereinzelte_stimmen": rec["vereinzelte_stimmen"],
            "kandidaten_nr": kandidaten_nr,
            "personen_id": cand["personen_id"],
            "bisher": empty_to_na(cand.get("bisher")),
            "gewahlt": cand["gewahlt"],
            "ganzer_name": cand["ganzer_name"],
            "name": cand["name"],
            "vorname": cand["vorname"],
            "geschlecht": cand["geschlecht"],
            "jahrgang": cand["jahrgang"],
            "anrede": cand["anrede"],
            "beruf": cand["beruf"],
            "heimatort": empty_to_na(cand.get("heimatort")),
            "strasse": empty_to_na(cand.get("strasse")),
            "plz": empty_to_na(cand.get("plz")),
            "ort": empty_to_na(cand.get("ort")),
            "stimmen": rec["stimmen"],
            "stimmen_prasident": cand.get("stimmen_prasident", pd.NA),
            "gewahlt_prasident": cand.get("gewahlt_prasident", pd.NA),
            "total_gultige_wahlzettel": rec["total_gultige_wahlzettel"],
            "stimmbeteiligung": maka["stimmbeteiligung"] if is_gemeinde_total else pd.NA,
            "anteil_brieflich_wahlende": anteil_brieflich(wahllokal, rec, maka, briefliche),
            "absolutes_mehr": to_number(absolutes_mehr_kanton) if is_kanton else pd.NA,
        }
        rows.append(out)

    df = pd.DataFrame(rows)
    for col in INT_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df[EXPORT_COLUMNS]


def briefliche_for_wahllokal(wahllokal, rec, maka):
    if wahllokal in PHYSICAL_URNE:
        return 0
    if wahllokal in BRIEFLICH_CHANNELS:
        return rec["wahlzettel"]
    if wahllokal in GEMEINDE_TOTALS:
        return to_number(maka["briefliche_stimmabgaben"])
    return pd.NA


def anteil_brieflich(wahllokal, rec, maka, briefliche):
    if wahllokal in GEMEINDE_TOTALS:
        return empty_to_na(maka.get("anteil_brieflich_wahlende"))
    eingelegte = rec["wahlzettel"]
    if pd.isna(eingelegte) or eingelegte in (0, "0"):
        return pd.NA
    if pd.isna(briefliche):
        return pd.NA
    ratio = float(briefliche) / float(eingelegte)
    return f"{ratio * 100:.2f}%"


def empty_to_na(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return pd.NA
    text = str(value).strip()
    return pd.NA if text == "" or text.lower() == "nan" else value


def validate_totals(df):
    """Gemeinde totals in Excel/MAKA must equal the sum of the remaining Wahllokale."""
    for gemeinde, total_name in (
        ("Stadt Basel", "Total Basel"),
        ("Gemeinde Riehen", "Total Riehen"),
        ("Gemeinde Bettingen", "Total Bettingen"),
    ):
        for kandidaten_nr, group in df[df["bezeichnung_wahlkreis"] == gemeinde].groupby("kandidaten_nr"):
            total_row = group[group["wahllokal"] == total_name]
            parts = group[~group["wahllokal"].isin(GEMEINDE_TOTALS)]
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
