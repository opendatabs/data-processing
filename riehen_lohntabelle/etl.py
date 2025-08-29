import logging
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pdfplumber

pd.set_option("future.no_silent_downcasting", True)
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logging.getLogger("pdfplumber").setLevel(logging.WARNING)

YEAR_RE = re.compile(r"(19|20)\d{2}")

def _pdfs_with_year(folder: Path):
    """Yield (year:int, pdf_path:Path) for all PDFs in folder (non-recursive)."""
    for p in sorted(folder.glob("*.pdf")):
        m = YEAR_RE.search(p.name)
        if not m:
            logging.warning("No year found in filename: %s", p.name)
            continue
        yield int(m.group(0)), p


def _extract_tables_verwaltung(pdf_path: Path) -> list[pd.DataFrame]:
    """Extract all tables from all pages of a PDF as DataFrames."""
    dfs: list[pd.DataFrame] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            # Try with line-based detection first; fall back to default.
            table = page.extract_table()
            df = pd.DataFrame(table[1:], columns=table[0]) if table else pd.DataFrame()
            df = df.dropna(how="any").reset_index(drop=True)
            dfs.append(df)
    return dfs

def _extract_tables_lehrpersonen(pdf_path: Path) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                dfs.append(pd.DataFrame()); continue

            df = pd.DataFrame(table[1:], columns=table[0])

            # --- Clean empty cols/rows (no .str on DataFrame) ---
            norm = lambda x: "" if pd.isna(x) else str(x).replace("\u00a0"," ").strip()
            stripped = df.applymap(norm)
            df = df.loc[:, (stripped != "").any(axis=0)].reset_index(drop=True)

            # --- Find the true header row (max # of "Stufe") ---
            def row_list(i): return [norm(x) for x in df.iloc[i].tolist()]
            header_idx, best = None, -1
            for i in range(len(df)):
                r = row_list(i)
                c = sum(1 for x in r if x == "Stufe")
                if c > best:
                    best, header_idx = c, i
            if header_idx is None or best <= 0:
                dfs.append(pd.DataFrame()); continue

            header_row = row_list(header_idx)

            # --- Block starts (tolerant) ---
            wanted = ["Stufe","Lohnverlauf","CHF","Faktor","%"]
            starts = []
            j = 0
            while j < len(header_row):
                if header_row[j] == "Stufe":
                    nxt = header_row[j:j+5]
                    if sum(a == b for a, b in zip(nxt, wanted)) >= 3:
                        starts.append(j); j += 5; continue
                j += 1
            if not starts:
                dfs.append(pd.DataFrame()); continue

            # --- Map metadata rows by label anywhere before header ---
            meta_labels = ["Typ","Zielfunktion","AN","spez. Lohnkurve"]
            meta_row_idx: dict[str,int] = {}
            for i in range(header_idx):
                r = row_list(i)
                for lab in meta_labels:
                    if lab in r and lab not in meta_row_idx:
                        meta_row_idx[lab] = i

            # Prefer value to the right of the label; ignore the label token itself
            label_tokens = set(meta_labels + ["spez_Lohnkurve"])
            def pick_meta(label: str, lo: int, hi: int) -> str | None:
                mi = meta_row_idx.get(label)
                if mi is None: return None

                # First try inside the block slice
                row_slice = [norm(x) for x in df.iloc[mi, slice(lo, hi)].tolist()]
                for v in row_slice:
                    if v and v not in label_tokens:
                        return v

                # Fallback: search the full row to the right of the label cell
                full = [norm(x) for x in df.iloc[mi].tolist()]
                try:
                    pos = full.index(label)
                    for v in full[pos+1:]:
                        if v and v not in label_tokens:
                            return v
                except ValueError:
                    pass

                # Last resort: first non-empty that isn't a label token
                for v in full:
                    if v and v not in label_tokens:
                        return v
                return None

            # --- Data region ---
            data = df.iloc[header_idx+1:].copy()
            stripped_data = data.applymap(norm)
            data = data.loc[(stripped_data != "").any(axis=1)]
            if data.empty:
                dfs.append(pd.DataFrame()); continue

            # --- Build blocks ---
            parts = []
            for start in starts:
                lo, hi = start, min(start+5, df.shape[1])
                sub = data.iloc[:, slice(lo, hi)].copy()
                if sub.shape[1] < 3:  # too noisy
                    continue

                # Rename columns to canonical names
                cols = ["Stufe","Lohnverlauf","CHF","Faktor","%"][:sub.shape[1]]
                sub.columns = cols

                # Numeric cleanup
                def clean_num(s):
                    if pd.isna(s): return s
                    t = str(s).replace("’","").replace("'","").replace("\u2009","")
                    t = t.replace("\u00a0","").replace(" ", "").replace(",", ".")
                    return t
                for c in ("Stufe","Lohnverlauf","CHF","Faktor"):
                    if c in sub.columns:
                        sub[c] = pd.to_numeric(sub[c].map(clean_num), errors="coerce")
                        if c == "Stufe":
                            sub[c] = sub[c].astype("Int64")

                sub["Prozent"] = sub.get("%", pd.NA)
                if "%" in sub.columns: sub = sub.drop(columns=["%"])

                # --- Attach metadata (now correctly picking the VALUES) ---
                meta_rec = {
                    "Typ":            pick_meta("Typ", lo, hi),
                    "Zielfunktion":   pick_meta("Zielfunktion", lo, hi),
                    "AN":             pick_meta("AN", lo, hi),
                    "spez_Lohnkurve": pick_meta("spez. Lohnkurve", lo, hi) or pick_meta("spez_Lohnkurve", lo, hi),
                }
                for k, v in meta_rec.items():
                    sub[k] = v

                # Reorder
                want = ["Typ","Zielfunktion","AN","spez_Lohnkurve",
                        "Stufe","Lohnverlauf","CHF","Faktor","Prozent"]
                for w in want:
                    if w not in sub.columns: sub[w] = pd.NA
                parts.append(sub[want])

            dfs.append(pd.concat(parts, ignore_index=True) if parts else pd.DataFrame())
    return dfs



def _process_folder(folder: Path) -> pd.DataFrame:
    """Combine tables from all PDFs in a folder, tagging with 'Jahr' column."""
    all_parts: list[pd.DataFrame] = []
    for jahr, pdf_path in _pdfs_with_year(folder):
        try:
            tables = _extract_tables_verwaltung(pdf_path) if folder.name == "verwaltung" else _extract_tables_lehrpersonen(pdf_path)
            if not tables:
                logging.warning("No tables found in %s", pdf_path.name)
                continue
            for df in tables:
                df = df.copy()
                df["Jahr"] = jahr
                all_parts.append(df)
        except Exception as e:
            logging.exception("Failed on %s: %s", pdf_path, e)
    if not all_parts:
        return pd.DataFrame()
    # Outer concat to keep all columns that occur in any table
    out = pd.concat(all_parts, ignore_index=True, sort=False)
    return out

def get_lohntabellen(folders: list[str]) -> dict[str, pd.DataFrame]:
    data_orig = Path("data_orig")
    results: dict[str, pd.DataFrame] = {}
    for name in folders:
        folder = data_orig / name
        if not folder.exists():
            logging.warning("Folder not found: %s", folder)
            results[name] = pd.DataFrame()
            continue
        logging.info("Processing folder: %s", folder)
        results[name] = _process_folder(folder)
    return results

def main():
    data_dir = Path("data")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Adjust the two folder names here as needed:
    folders = ["verwaltung", "lehrpersonen"] 
    results = get_lohntabellen(folders)

    for name, df in results.items():
        out_path = data_dir / f"{name}.csv"
        if df.empty:
            logging.warning("No data extracted for '%s'; skipping save.", name)
            continue

        df.to_csv(out_path, index=False)
        logging.info("Saved %s (%d rows, %d cols)", out_path, len(df), df.shape[1])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
    logging.info("Job successful!")
