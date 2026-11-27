"""Write poll results as one CSV per session day and mirror them to the FTP folder that feeds dataset 100186.

Layout (local and remote):
    100186_daten/Amtsjahr_2024-2025/20240417_gribs_json.csv

- One file per session day and source system ("quelle"): gribs_json (current system, since 2023-09),
  archiv_xml (old system until 2023-06), congress_center (2021-02 to 2022-02). No session day is in more than one source.
- Each day file is always rebuilt completely from the source, so polls deleted at the source disappear.
- With prune=True, files of that source which are no longer produced (e.g. a deleted session day) are deleted on FTP.
"""

import ftplib
import logging
import os
from pathlib import Path

import common
import common.change_tracking as ct
import pandas as pd

LOCAL_BASE = Path("data") / "100186_daten"
REMOTE_BASE = "parlamentsdienst/gr_abstimmungsergebnisse/100186_daten"
DATASET_ID = "100186"
QUELLEN = ("gribs_json", "archiv_xml", "congress_center")

OUTPUT_COLUMNS = [
    "session_date",
    "Abst_Nr",
    "Datum",
    "Zeit",
    "Anz_J",
    "Anz_N",
    "Anz_E",
    "Anz_A",
    "Anz_P",
    "Typ",
    "Geschaeft",
    "laufnr_ges",
    "signatur_ges",
    "signatur_dok",
    "url_ges",
    "url_geschaeft_ods",
    "url_dokument_ods",
    "Zeitstempel",
    "Zeitstempel_text",
    "Sitz_Nr",
    "Mitglied_Anrede",
    "Mitglied_Name",
    "Fraktion",
    "Mitglied_Name_Fraktion",
    "Mitglied_Funktion",
    "Datenstand",
    "Datenstand_text",
    "Entscheid_Mitglied",
    "Traktandum",
    "Subtraktandum",
    "tagesordnung_link",
    "Mitglied_Vorname",
    "Mitglied_Nachname",
    "GR_uni_nr",
    "GR_url",
    "GR_url_ods",
    "anr",
    "url_abstimmung_dok",
]


def amtsjahr(session_date: str) -> str:
    """'YYYYMMDD' -> 'Amtsjahr_YYYY-YYYY'. The Amtsjahr of the Grosser Rat starts on February 1st."""
    year, month = int(session_date[:4]), int(session_date[4:6])
    start = year if month >= 2 else year - 1
    return f"Amtsjahr_{start}-{start + 1}"


def day_file_name(session_date: str, quelle: str) -> str:
    return f"{session_date}_{quelle}.csv"


def relative_path(session_date: str, quelle: str) -> str:
    """Path relative to the base folder, always with '/' (used for local and remote paths)."""
    return f"{amtsjahr(session_date)}/{day_file_name(session_date, quelle)}"


def write_day_files(df: pd.DataFrame, quelle: str) -> list[str]:
    """Write one CSV per session day. Returns the relative paths of the written files."""
    if quelle not in QUELLEN:
        raise ValueError(f"Unknown quelle {quelle}, expected one of {QUELLEN}")
    if df is None or df.empty:
        logging.info(f"No polls for quelle {quelle}, no day files written.")
        return []
    df = df.copy()
    df["session_date"] = df["session_date"].astype(str)
    written = []
    for session_date, df_day in df.groupby("session_date"):
        df_day = df_day.reindex(columns=OUTPUT_COLUMNS)
        # Stable order so that unchanged data gives a byte-identical file (change tracking)
        df_day = df_day.assign(
            _abst=pd.to_numeric(df_day["Abst_Nr"], errors="coerce"),
            _sitz=pd.to_numeric(df_day["Sitz_Nr"], errors="coerce"),
        ).sort_values(["_abst", "_sitz"], kind="stable")
        df_day = df_day.drop(columns=["_abst", "_sitz"])
        rel = relative_path(session_date, quelle)
        local_file = LOCAL_BASE / rel
        local_file.parent.mkdir(parents=True, exist_ok=True)
        df_day.to_csv(local_file, index=False)
        written.append(rel)
    logging.info(f"Wrote {len(written)} day file(s) for quelle {quelle} to {LOCAL_BASE}.")
    return written


def _ensure_remote_dir(ftp: ftplib.FTP, path: str) -> None:
    try:
        ftp.mkd(path)
    except ftplib.error_perm as e:
        # 550: exists already (wording differs between servers)
        if not str(e).startswith("550"):
            raise


def _list_remote_day_files(ftp: ftplib.FTP, quelle: str) -> set[str]:
    """Relative paths of all remote day files of one source, e.g. {'Amtsjahr_2024-2025/20240417_gribs_json.csv'}."""
    suffix = f"_{quelle}.csv"
    result = set()
    for folder in ftp.nlst(REMOTE_BASE):
        folder_name = folder.rsplit("/", 1)[-1]
        if not folder_name.startswith("Amtsjahr_"):
            continue
        for entry in ftp.nlst(f"{REMOTE_BASE}/{folder_name}"):
            name = entry.rsplit("/", 1)[-1]
            if name.endswith(suffix):
                result.add(f"{folder_name}/{name}")
    return result


def files_to_delete(remote: set[str], written: list[str]) -> list[str]:
    return sorted(remote - set(written))


def _remove_local(rel: str) -> None:
    local_file = LOCAL_BASE / rel
    if local_file.exists():
        local_file.unlink()
    check_file = ct.get_check_file(str(local_file), ct.get_check_file_dir())
    if os.path.exists(check_file):
        os.remove(check_file)


def sync_to_ftp(written: list[str], quelle: str, prune: bool) -> bool:
    """Upload changed day files; with prune=True delete remote files of this source that were not written.

    Returns True if anything changed on FTP.
    """
    changed = [rel for rel in written if ct.has_changed(str(LOCAL_BASE / rel))]
    if not changed and not prune:
        logging.info(f"No changed day files for quelle {quelle}.")
        return False

    ftp = ftplib.FTP(common.FTP_SERVER, common.FTP_USER, common.FTP_PASS)
    try:
        _ensure_remote_dir(ftp, REMOTE_BASE)
        for folder in sorted({rel.split("/")[0] for rel in changed}):
            _ensure_remote_dir(ftp, f"{REMOTE_BASE}/{folder}")
        for rel in changed:
            local_file = LOCAL_BASE / rel
            logging.info(f"Uploading {local_file} to {REMOTE_BASE}/{rel}...")
            with open(local_file, "rb") as f:
                ftp.storbinary(f"STOR {REMOTE_BASE}/{rel}", f)
            ct.update_hash_file(str(local_file))

        deleted = []
        if prune:
            for rel in files_to_delete(_list_remote_day_files(ftp, quelle), written):
                logging.info(f"Deleting {REMOTE_BASE}/{rel} (no longer in source {quelle})...")
                try:
                    ftp.delete(f"{REMOTE_BASE}/{rel}")
                except ftplib.error_perm as e:
                    if not str(e).startswith("550"):
                        raise
                _remove_local(rel)
                deleted.append(rel)
    finally:
        ftp.quit()

    logging.info(f"Quelle {quelle}: {len(changed)} file(s) uploaded, {len(deleted)} file(s) deleted.")
    return bool(changed or deleted)
