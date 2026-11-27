"""Report test polls removed by poll_filters.clean_polls by e-mail, with an Excel list attached.

- Every removed test poll is identified by (Datum, Zeit, Abst_Nr).
- A lookup table (LOOKUP_FILE) holds all test polls already reported. An e-mail is only sent if there are test polls
  that are not in the lookup table yet, so a job that runs every few minutes does not send the same mail again.
- The lookup table is only updated after the e-mail has been sent; if sending fails, it is tried again on the next run.
- Never fails the ETL.
"""

import logging
from datetime import date
from pathlib import Path

import common
import pandas as pd
import poll_filters
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

LOOKUP_FILE = Path("change_tracking") / "herausgefilterte_abstimmungen.csv"
EXCEL_FILE = Path("data") / "herausgefilterte_testabstimmungen.xlsx"
KEY_COLUMNS = ["Datum", "Zeit", "Abst_Nr"]
LOOKUP_COLUMNS = [*poll_filters.REMOVED_POLL_COLUMNS, "Quelle", "Gemeldet_am"]
REASON = f"ausserhalb der Sitzungszeit und mehr als {poll_filters.MAX_ABSENT} Abwesende"

# Column names in the Excel file (in this order)
EXCEL_COLUMNS = {
    "Datum": "Datum",
    "Zeit": "Zeit",
    "Abst_Nr": "Abst. Nr.",
    "Anz_J": "Ja",
    "Anz_N": "Nein",
    "Anz_E": "Enthaltungen",
    "Anz_A": "Abwesend",
    "Anz_P": "Präsidium",
    "Typ": "Typ",
    "Geschaeft": "Geschäft",
    "signatur_ges": "Signatur Geschäft",
    "Traktandum": "Traktandum",
    "Subtraktandum": "Subtraktandum",
    "Sitzungen_Kalender": "Sitzungen laut Kalender",
    "Grund": "Grund",
    "Quelle": "Quelle",
    "Gemeldet_am": "Gemeldet am",
}
WRAP_COLUMNS = {"Geschäft", "Sitzungen laut Kalender", "Grund"}
MAX_COLUMN_WIDTH = 60

RULES_TEXT = [
    "Herausgefilterte Testabstimmungen des Grossen Rats (Datensatz 100186)",
    "",
    "Regeln (gemäss Parlamentsdienst):",
    "- Sitzungszeit = 09:00-12:30, 15:00-18:30, 20:00-23:30 Uhr oder eine Sitzung im Sitzungskalender "
    "von Beginn bis Ende + 30 Minuten (abgesagte Sitzungen zählen nicht).",
    f"- Mehr als {poll_filters.MAX_ABSENT} Abwesende = nicht beschlussfähig.",
    f"- Ausserhalb der Sitzungszeit und mehr als {poll_filters.MAX_ABSENT} Abwesende: Testabstimmung, "
    "wird nicht publiziert (diese Liste).",
    f"- Innerhalb der Sitzungszeit und mehr als {poll_filters.MAX_ABSENT} Abwesende: Fehlabstimmung, "
    f"wird publiziert mit Typ '{poll_filters.TYP_INVALID}'.",
    "- Abstimmungen an Tagen ohne Sitzung im Kalender werden ebenfalls nicht publiziert (nicht in dieser Liste).",
    "",
    "Blätter:",
    "- 'Neu herausgefiltert': Abstimmungen, die mit dieser Mail zum ersten Mal gemeldet werden.",
    "- 'Alle herausgefiltert': alle bisher gemeldeten Testabstimmungen.",
]


def _poll_keys(df: pd.DataFrame) -> pd.Series:
    return df[KEY_COLUMNS].astype(str).agg("|".join, axis=1)


def load_lookup(path: Path | str = LOOKUP_FILE) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame(columns=LOOKUP_COLUMNS)
    return pd.read_csv(path, dtype=str, keep_default_na=False).reindex(columns=LOOKUP_COLUMNS)


def save_lookup(df: pd.DataFrame, path: Path | str = LOOKUP_FILE) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def combine(removed_by_quelle: dict[str, list[pd.DataFrame]]) -> pd.DataFrame:
    """{'gribs_json': [df, ...], ...} -> one dataframe with column 'Quelle', one row per poll, all values as str."""
    frames = [df.assign(Quelle=quelle) for quelle, dfs in removed_by_quelle.items() for df in dfs if not df.empty]
    if not frames:
        return pd.DataFrame(columns=LOOKUP_COLUMNS)
    df = pd.concat(frames, ignore_index=True).reindex(columns=LOOKUP_COLUMNS)
    df = df.astype(str).replace({"nan": "", "None": "", "<NA>": ""})
    return df.drop_duplicates(subset=KEY_COLUMNS).reset_index(drop=True)


def _format_seconds(secs: float) -> str:
    secs = int(secs)
    return f"{secs // 3600:02d}:{secs % 3600 // 60:02d}"


def calendar_sessions_text(datum: str, windows: pd.DataFrame | None) -> str:
    """Single sessions of a day in the calendar, e.g. '08:00-12:00, 14:00-18:00' (end without the 30 min buffer)."""
    if windows is None or windows.empty:
        return ""
    day = windows[windows["Datum"] == datum].sort_values("start_s")
    if day.empty:
        return "keine Sitzung im Kalender"
    buffer_s = poll_filters.OVERRUN_BUFFER.total_seconds()
    return ", ".join(f"{_format_seconds(r.start_s)}-{_format_seconds(r.end_s - buffer_s)}" for r in day.itertuples())


def _to_excel_frame(df: pd.DataFrame, windows: pd.DataFrame | None) -> pd.DataFrame:
    df = df.sort_values(KEY_COLUMNS).copy()
    df["Sitzungen_Kalender"] = df["Datum"].map(lambda d: calendar_sessions_text(d, windows))
    df["Grund"] = REASON
    df["Zeit"] = df["Zeit"].str[:8]
    df = df.reindex(columns=list(EXCEL_COLUMNS)).rename(columns=EXCEL_COLUMNS)
    # Real dates and numbers, so that Excel can sort and filter them correctly
    for col in ["Datum", "Gemeldet am"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    for col in ["Abst. Nr.", "Ja", "Nein", "Enthaltungen", "Abwesend", "Präsidium"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _format_sheet(ws) -> None:
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for cell in ws[1]:
        cell.font = Font(bold=True)
    for col_idx, column in enumerate(ws.iter_cols(), start=1):
        header = column[0].value
        width = max(len(str(c.value)) if c.value is not None else 0 for c in column)
        ws.column_dimensions[get_column_letter(col_idx)].width = min(width + 2, MAX_COLUMN_WIDTH)
        for cell in column[1:]:
            if header in ("Datum", "Gemeldet am"):
                cell.number_format = "DD.MM.YYYY"
            if header in WRAP_COLUMNS:
                cell.alignment = Alignment(wrap_text=True, vertical="top")
            else:
                cell.alignment = Alignment(vertical="top")


def write_excel(
    df_new: pd.DataFrame, df_all: pd.DataFrame, windows: pd.DataFrame | None, path: Path | str = EXCEL_FILE
) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in [("Neu herausgefiltert", df_new), ("Alle herausgefiltert", df_all)]:
            _to_excel_frame(df, windows).to_excel(writer, sheet_name=sheet_name, index=False)
            _format_sheet(writer.sheets[sheet_name])
        pd.DataFrame({"Erläuterung": RULES_TEXT}).to_excel(writer, sheet_name="Regeln", index=False, header=False)
        writer.sheets["Regeln"].column_dimensions["A"].width = 120
    return path


def build_email_text(df_new: pd.DataFrame, windows: pd.DataFrame | None) -> str:
    text = "Guten Tag\n\n"
    text += (
        f"Folgende {len(df_new)} Abstimmung(en) des Grossen Rats wurden als Testabstimmung erkannt "
        f"({REASON}) und werden nicht im Datensatz 100186 publiziert:\n\n"
    )
    for r in df_new.sort_values(KEY_COLUMNS).itertuples():
        datum = pd.to_datetime(r.Datum).strftime("%d.%m.%Y")
        sessions = calendar_sessions_text(r.Datum, windows)
        text += f" - {datum} {r.Zeit[:8]}, Abst. Nr. {r.Abst_Nr}, {r.Anz_A} abwesend"
        text += f" (Sitzungen laut Kalender: {sessions})" if sessions else ""
        text += f": {r.Geschaeft}\n" if r.Geschaeft else "\n"
    text += (
        "\nIm Anhang befindet sich eine Excel-Datei mit diesen und allen bisher herausgefilterten Testabstimmungen "
        "sowie den angewendeten Regeln.\n"
        "Falls eine echte Abstimmung darunter ist, muss der Filter (poll_filters.py) angepasst werden.\n"
    )
    text += "\nFreundliche Grüsse, \nEuer automatisierter Open Data Basel-Stadt Python Job"
    return text


def notify_removed_polls(
    removed_by_quelle: dict[str, list[pd.DataFrame]],
    windows: pd.DataFrame | None = None,
    *,
    lookup_path: Path | str = LOOKUP_FILE,
    excel_path: Path | str = EXCEL_FILE,
) -> bool:
    """Send an e-mail with an Excel list if test polls were removed that have not been reported yet.

    Returns True if an e-mail was sent. Never raises.
    """
    try:
        df_removed = combine(removed_by_quelle)
        df_lookup = load_lookup(lookup_path)
        df_new = df_removed[~_poll_keys(df_removed).isin(set(_poll_keys(df_lookup)))].copy()
        if df_new.empty:
            logging.info(
                f"{len(df_removed)} test poll(s) removed in this run, all reported before; no e-mail sent."
                if len(df_removed)
                else "No test polls removed in this run; no e-mail sent."
            )
            return False

        df_new["Gemeldet_am"] = date.today().isoformat()
        df_all = pd.concat([df_lookup, df_new], ignore_index=True)
        excel_file = write_excel(df_new, df_all, windows, excel_path)
        logging.info(f"{len(df_new)} new test poll(s) removed, sending e-mail with {excel_file}...")
        msg = common.email_message(
            subject=f"Grosser Rat Abstimmungen (100186): {len(df_new)} Testabstimmung(en) herausgefiltert",
            text=build_email_text(df_new, windows),
            img=None,
            attachment=str(excel_file),
        )
        common.send_email(msg)
        save_lookup(df_all.sort_values(KEY_COLUMNS), lookup_path)
        logging.info("E-mail about removed test polls sent, lookup table updated.")
        return True
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logging.warning(f"Could not report removed test polls (will be retried on the next run): {exc}")
        return False
