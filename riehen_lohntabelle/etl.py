import logging
import re
from pathlib import Path

import common
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
    """
    Parse Verwaltung tables and return long format:
    AN, Stufe, Lohnverlauf, CHF, Faktor, Prozent
    (Jahr is added later in _process_folder)
    """
    dfs: list[pd.DataFrame] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                dfs.append(pd.DataFrame())
                continue

            df = pd.DataFrame(table[1:], columns=table[0]).copy()
            if df.empty or df.columns.size == 0:
                dfs.append(pd.DataFrame())
                continue

            # --- normalize header names ---
            cols = [str(c).strip().replace("\u00a0", " ") for c in df.columns]
            df.columns = cols

            # First column -> AN
            first_col = df.columns[0]
            df = df.rename(columns={first_col: "AN"})
            df["AN"] = df["AN"].astype(str).str.replace(r"^AN\s*", "", regex=True).str.strip()

            # numeric cleanup for all non-AN columns
            def to_num(s):
                if pd.isna(s):
                    return np.nan
                t = str(s)
                t = t.replace("’", "").replace("'", "").replace("\u2009", "").replace("\u00a0", "").replace(" ", "")
                t = t.replace(",", ".")
                return pd.to_numeric(t, errors="coerce")

            for col in df.columns[1:]:
                df[col] = df[col].map(to_num)

            # Identify "Erfa" columns; ignore "Jahr"
            erfa_cols = [c for c in df.columns if re.match(r"^\s*\d+\s*Erfa\s*$", str(c))]
            if not erfa_cols:
                # Sometimes headers are like "1Erfa" (no space)
                erfa_cols = [c for c in df.columns if re.match(r"^\s*\d+\s*Erfa", str(c))]

            # Melt to long
            long = df.melt(
                id_vars=["AN"],
                value_vars=erfa_cols,
                var_name="Stufe",
                value_name="Lohnverlauf",
            )

            # Clean AN and Stufe
            long["AN"] = pd.to_numeric(long["AN"], errors="coerce").astype("Int64")
            long["Stufe"] = long["Stufe"].astype(str).str.extract(r"(\d+)")[0].astype("Int64")

            # Drop empty Lohnverlauf rows
            long = long.dropna(subset=["Lohnverlauf"]).sort_values(["AN", "Stufe"])

            # Compute CHF (step delta), Faktor (vs first), Prozent (delta factor in %)
            def add_calcs(g: pd.DataFrame) -> pd.DataFrame:
                g = g.sort_values("Stufe").copy()
                base = g["Lohnverlauf"].iloc[0]
                g["CHF"] = g["Lohnverlauf"].diff()
                g.loc[g.index[0], "CHF"] = np.nan  # first step has no delta
                g["Faktor"] = g["Lohnverlauf"] / base
                g["Prozent"] = g["Faktor"].diff() * 100.0  # delta vs previous step
                g.loc[g.index[0], "Prozent"] = np.nan
                return g

            long = long.groupby("AN", group_keys=False).apply(add_calcs)

            # Order & types
            long = long[["AN", "Stufe", "Lohnverlauf", "CHF", "Faktor", "Prozent"]]
            long["Lohnverlauf"] = long["Lohnverlauf"].astype(float)
            long["CHF"] = long["CHF"].astype(float)
            long["Faktor"] = long["Faktor"].astype(float)
            long["Prozent"] = long["Prozent"].astype(float)

            dfs.append(long.reset_index(drop=True))
    return dfs


def _extract_tables_lehrpersonen(pdf_path: Path) -> list[pd.DataFrame]:
    dfs: list[pd.DataFrame] = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                dfs.append(pd.DataFrame())
                continue

            df = pd.DataFrame(table[1:], columns=table[0])

            # --- Clean empty cols/rows (no .str on DataFrame) ---
            def norm(x):
                if pd.isna(x):
                    return ""
                return str(x).replace("\u00a0", " ").strip()

            stripped = df.applymap(norm)
            df = df.loc[:, (stripped != "").any(axis=0)].reset_index(drop=True)

            # --- Find the true header row (max # of "Stufe") ---
            def row_list(i):
                return [norm(x) for x in df.iloc[i].tolist()]

            header_idx, best = None, -1
            for i in range(len(df)):
                r = row_list(i)
                c = sum(1 for x in r if x == "Stufe")
                if c > best:
                    best, header_idx = c, i
            if header_idx is None or best <= 0:
                dfs.append(pd.DataFrame())
                continue

            header_row = row_list(header_idx)

            # --- Block starts (tolerant) ---
            wanted = ["Stufe", "Lohnverlauf", "CHF", "Faktor", "%"]
            starts = []
            j = 0
            while j < len(header_row):
                if header_row[j] == "Stufe":
                    nxt = header_row[j : j + 5]
                    if sum(a == b for a, b in zip(nxt, wanted)) >= 3:
                        starts.append(j)
                        j += 5
                        continue
                j += 1
            if not starts:
                dfs.append(pd.DataFrame())
                continue

            # --- Map metadata rows by label anywhere before header ---
            meta_labels = ["Typ", "Zielfunktion", "AN", "spez. Lohnkurve"]
            meta_row_idx: dict[str, int] = {}
            for i in range(header_idx):
                r = row_list(i)
                for lab in meta_labels:
                    if lab in r and lab not in meta_row_idx:
                        meta_row_idx[lab] = i

            # Prefer value to the right of the label; ignore the label token itself
            label_tokens = set(meta_labels + ["spez_Lohnkurve"])

            def pick_meta(label: str, lo: int, hi: int) -> str | None:
                mi = meta_row_idx.get(label)
                if mi is None:
                    return None

                # First try inside the block slice
                row_slice = [norm(x) for x in df.iloc[mi, slice(lo, hi)].tolist()]
                for v in row_slice:
                    if v and v not in label_tokens:
                        return v

                # Fallback: search the full row to the right of the label cell
                full = [norm(x) for x in df.iloc[mi].tolist()]
                try:
                    pos = full.index(label)
                    for v in full[pos + 1 :]:
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
            data = df.iloc[header_idx + 1 :].copy()
            stripped_data = data.applymap(norm)
            data = data.loc[(stripped_data != "").any(axis=1)]
            if data.empty:
                dfs.append(pd.DataFrame())
                continue

            # --- Build blocks ---
            parts = []
            for start in starts:
                lo, hi = start, min(start + 5, df.shape[1])
                sub = data.iloc[:, slice(lo, hi)].copy()
                if sub.shape[1] < 3:  # too noisy
                    continue

                # Rename columns to canonical names
                cols = ["Stufe", "Lohnverlauf", "CHF", "Faktor", "%"][: sub.shape[1]]
                sub.columns = cols

                # Numeric cleanup
                def clean_num(s):
                    if pd.isna(s):
                        return s
                    t = str(s).replace("’", "").replace("'", "").replace("\u2009", "")
                    t = t.replace("\u00a0", "").replace(" ", "").replace(",", ".")
                    return t

                for c in ("Stufe", "Lohnverlauf", "CHF", "Faktor"):
                    if c in sub.columns:
                        sub[c] = pd.to_numeric(sub[c].map(clean_num), errors="coerce")
                        if c == "Stufe":
                            sub[c] = sub[c].astype("Int64")

                sub["Prozent"] = sub.get("%", pd.NA)
                if "%" in sub.columns:
                    sub = sub.drop(columns=["%"])
                # Remove the % sign if present
                if sub["Prozent"].dtype == object:
                    sub["Prozent"] = sub["Prozent"].map(lambda x: str(x).replace("%", "").strip() if pd.notna(x) else x)
                    sub["Prozent"] = pd.to_numeric(sub["Prozent"].map(clean_num), errors="coerce")

                # --- Attach metadata---
                meta_rec = {
                    "Typ": pick_meta("Typ", lo, hi),
                    "Zielfunktion": pick_meta("Zielfunktion", lo, hi),
                    "AN": pick_meta("AN", lo, hi),
                    "spez_Lohnkurve": pick_meta("spez. Lohnkurve", lo, hi) or pick_meta("spez_Lohnkurve", lo, hi),
                }
                for k, v in meta_rec.items():
                    sub[k] = v

                # Reorder
                want = [
                    "Typ",
                    "Zielfunktion",
                    "AN",
                    "spez_Lohnkurve",
                    "Stufe",
                    "Lohnverlauf",
                    "CHF",
                    "Faktor",
                    "Prozent",
                ]
                for w in want:
                    if w not in sub.columns:
                        sub[w] = pd.NA
                parts.append(sub[want])

            dfs.append(pd.concat(parts, ignore_index=True) if parts else pd.DataFrame())
    return dfs


def _process_folder(folder: Path) -> pd.DataFrame:
    """Combine tables from all PDFs in a folder, tagging with 'Jahr' column."""
    all_parts: list[pd.DataFrame] = []
    for jahr, pdf_path in _pdfs_with_year(folder):
        try:
            tables = (
                _extract_tables_verwaltung(pdf_path)
                if folder.name == "verwaltung"
                else _extract_tables_lehrpersonen(pdf_path)
            )
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
    # Adjust the two folder names here as needed:
    folders = ["verwaltung", "lehrpersonen"]
    ods_ids = ["100451", "100452"]
    results = get_lohntabellen(folders)

    for name, df in results.items():
        ods_id = ods_ids[folders.index(name)]
        out_path = f"data/{ods_id}_lohntabelle_{name}.csv"
        if df.empty:
            logging.warning("No data extracted for '%s'; skipping save.", name)
            continue

        df.to_csv(out_path, index=False)
        logging.info("Saved %s (%d rows, %d cols)", out_path, len(df), df.shape[1])
        common.update_ftp_and_odsp(out_path, "riehen/lohntabelle", ods_id)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    main()
    logging.info("Job successful!")
