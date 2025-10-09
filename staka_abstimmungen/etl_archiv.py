# etl_archiv.py
import os
import re
import glob
from collections import defaultdict, namedtuple
from datetime import datetime

import numpy as np
import pandas as pd
import dateparser

# ====== Config ======
IN_DIR = os.path.join("data", "github_2015-2020")
OUT_DIR = os.path.join("data", "data-processing-output")
WAHLLOKALE_CSV = os.path.join("data", "Wahllokale.csv")

# Gemeinde canonical names and IDs for Kennzahlen
GEMEIN_NAME_FIX = {
    "Total Basel": "Basel",
    "Basel": "Basel",
    "Total Riehen": "Riehen",
    "Riehen": "Riehen",
    "Total Bettingen": "Bettingen",
    "Bettingen": "Bettingen",
    "Total Auslandschweizer (AS)": "Auslandschweizer/-innen",
    "Auslandschweizer/-innen": "Auslandschweizer/-innen",
    "Total Kanton": "Basel-Stadt",
    "Basel-Stadt": "Basel-Stadt",
}
GEMEIN_IDS = {
    "Basel": 1,
    "Riehen": 2,
    "Bettingen": 3,
    "Auslandschweizer/-innen": 9,
    "Basel-Stadt": 99,
}

# ====== Helpers ======
def clean_num(x):
    if x is None:
        return pd.NA
    s = str(x).strip().replace("\u00a0", " ").replace(" ", "")
    if s == "" or s == "-":
        return pd.NA
    s = s.replace("'", "").replace("\u2009", "").replace(",", "")
    try:
        return int(s)
    except Exception:
        try:
            return int(float(s))
        except Exception:
            return pd.NA

def clean_numeric(s):
    if s is None:
        return pd.NA
    t = str(s).strip().replace("\u00a0"," ").replace(" ", "").replace("'", "")
    t = t.replace(",", ".")
    if t.endswith("%"):
        t = t[:-1]
        try:
            v = float(t)
            return v/100.0
        except:
            return pd.NA
    try:
        return float(t)
    except:
        return pd.NA

def format_title(core, has_geg):
    core = strip_quotes(core)
    base = f"«{core}»"
    return f"{base} und Gegenvorschlag" if has_geg else base

def find_in_hdrs(label, hdrs):
    label = label.lower()
    for hdr in hdrs:
        for j, cell in enumerate(hdr):
            if label in (cell or "").lower():
                return j
    return None

def nz(x):  # 0 for NA
    return 0 if pd.isna(x) else x

def wrap_guillemets(s):
    s = str(s).strip()
    return s if (s.startswith("«") and s.endswith("»")) else f"«{s}»"

def strip_quotes(s):
    return str(s).strip().strip("“”\"' ").replace("«", "").replace("»", "").strip()

def tokenize_csv_line(ln):
    return [c.strip() for c in ln.rstrip("\n").split(",")]

def detect_meta(lines):
    abst_art, abst_date = None, None
    for ln in lines[:30]:
        t = ln.strip()
        if "Abstimmung vom" in t or "Volksabstimmung vom" in t:
            left = t.split(",")[0]
            m = re.search(r"(Kantonale|Eidgen[öo]ssische).*?vom\s+(.*)$", left, re.IGNORECASE)
            if m:
                kind = m.group(1).lower()
                abst_art = "kantonal" if "kanton" in kind else "national"
                date_raw = m.group(2).strip()
                abst_date = dateparser.parse(date_raw).strftime("%Y-%m-%d")
                break
    return abst_art, abst_date

Section = namedtuple("Section", ["idx", "title", "is_gv"])

def find_sections(lines):
    secs = []
    for i, ln in enumerate(lines):
        m = re.match(r"^\((\d+)\)\s*«([^»]+)»(.*)$", ln.strip())
        if m:
            title_core = strip_quotes(m.group(2))  # no guillemets, no suffix
            tail = m.group(3) or ""
            is_gv = ("Gegen" in tail) or ("Stich" in tail)
            secs.append(Section(i, title_core, is_gv))
    return secs

def read_block(lines, start_idx):
    """
    From the section line, locate the header row that starts with 'Wahllokale',
    collect up to 4 non-empty pre-header lines, then read data rows until:
      - blank line
      - next '(n) «... »' section
      - Stimmberechtigte / Stimmbeteiligung / Abstimmungs-Kennzahlen block
    Return (pre_hdrs, hdr, rows, end_i)
    """
    i = start_idx + 1
    pre_hdrs, hdr, data = [], None, []

    # find header row
    while i < len(lines):
        s = lines[i].strip()
        if s == "" or re.match(r"^\(\d+\)\s*«", s):
            return [], None, [], i
        low = s.lower()
        if ("stimmberechtigte" in low) or ("stimmbeteiligung" in low) or ("abstimmungs-kennzahlen" in low):
            return [], None, [], i
        row = tokenize_csv_line(lines[i])
        if (row and (row[0] or "").startswith("Wahllokale")):
            # collect up to 4 non-empty previous lines
            j, buf = i - 1, []
            while j >= 0 and len(buf) < 4:
                r = tokenize_csv_line(lines[j])
                if any((c or "").strip() for c in r):
                    buf.append(r)
                j -= 1
            pre_hdrs = list(reversed(buf))
            hdr = row
            i += 1
            break
        i += 1

    if hdr is None:
        return [], None, [], i

    # read data rows
    while i < len(lines):
        s = lines[i].strip()
        if s == "" or re.match(r"^\(\d+\)\s*«", s):
            break
        low = s.lower()
        if ("stimmberechtigte" in low) or ("stimmbeteiligung" in low) or ("abstimmungs-kennzahlen" in low):
            break
        row = tokenize_csv_line(lines[i])
        if any((c or "").strip() for c in row):
            data.append(row)
        i += 1

    return pre_hdrs, hdr, data, i

def locate_indices_non_gv(hdr, pre_hdrs):
    idx = {"wahllok": 0}
    idx["stimmr"] = find_in_hdrs("stimmrechtsausweise", [hdr] + pre_hdrs)
    idx["eingel"] = find_in_hdrs("eingelegte", [hdr])
    idx["leer"] = find_in_hdrs("leere", [hdr])
    idx["unguelt"] = (find_in_hdrs("ungültige", [hdr]) or
                      find_in_hdrs("ungueltige", [hdr]) or
                      find_in_hdrs("ungultige", [hdr]))
    idx["guelt"] = (find_in_hdrs("total gültige", [hdr]) or
                    find_in_hdrs("total gültig", [hdr])  or
                    find_in_hdrs("gültige", [hdr])       or
                    find_in_hdrs("gueltige", [hdr]))
    idx["ja"] = find_in_hdrs("ja", [hdr])
    idx["nein"] = find_in_hdrs("nein", [hdr])
    return idx

def locate_indices_gv(pre_hdrs, hdr):
    idx = locate_indices_non_gv(hdr, pre_hdrs)

    gv_hdr = None
    for r in reversed(pre_hdrs):
        c0 = (r[0] or "").strip() if r else ""
        joined = " ".join([c or "" for c in r]).lower()
        if any(k in joined for k in ["initiative", "gegenvorschlag", "stichfrage"]) and not c0.startswith("("):
            gv_hdr = r
            break
    if gv_hdr is None:
        for r in reversed(pre_hdrs):
            joined = " ".join([c or "" for c in r]).lower()
            if any(k in joined for k in ["initiative", "gegenvorschlag", "stichfrage"]):
                gv_hdr = r
                break
    if not gv_hdr:
        return idx

    def block_pos(keyword):
        kw = keyword.lower()
        for k in range(1, len(gv_hdr)):  # skip possible "(n) …"
            v = gv_hdr[k]
            if kw in (v or "").lower():
                return k
        return None

    def find_in_range(lbl, start, end):
        if start is None: return None
        end = end if end is not None else len(hdr)
        lbl = lbl.lower()
        for j in range(start, end):
            if lbl in (hdr[j] or "").lower():
                return j
        return None

    pos_init = block_pos("initiative")
    pos_gege = block_pos("gegen")
    pos_sti  = block_pos("stich")

    all_pos = [p for p in [pos_init, pos_gege, pos_sti] if p is not None]
    all_pos.sort()
    def next_after(p): return next((q for q in all_pos if q > p), len(hdr))

    if pos_init is not None:
        a, b = pos_init, next_after(pos_init)
        idx["init_oga"]  = find_in_range("ohne", a, b)
        idx["init_ja"]   = find_in_range("ja", a, b)
        idx["init_nein"] = find_in_range("nein", a, b)
    if pos_gege is not None:
        a, b = pos_gege, next_after(pos_gege)
        idx["gege_oga"]  = find_in_range("ohne", a, b)
        idx["gege_ja"]   = find_in_range("ja", a, b)
        idx["gege_nein"] = find_in_range("nein", a, b)
    if pos_sti is not None:
        a, b = pos_sti, next_after(pos_sti)
        idx["sti_oga"]   = find_in_range("ohne", a, b)
        idx["sti_init"]  = find_in_range("initiative", a, b)
        idx["sti_gege"]  = find_in_range("gegen", a, b)

    return idx

def infer_indices_from_layout(first_data_row):
    """
    Fallback when labels fail: infer numeric columns order.
    Assumes canonical order ending with ... , Total gültige, Ja, Nein
    """
    nums = []
    for j, cell in enumerate(first_data_row):
        v = clean_num(cell)
        if not pd.isna(v):
            nums.append(j)

    idx = {"wahllok": 0, "stimmr": None, "eingel": None, "leer": None, "unguelt": None, "guelt": None, "ja": None, "nein": None}
    if len(nums) >= 6:
        idx["ja"]   = nums[-2]
        idx["nein"] = nums[-1]
        idx["guelt"] = nums[-3]
        base = nums[:-3]
        if len(base) >= 3:
            idx["eingel"], idx["leer"], idx["unguelt"] = base[-3], base[-2], base[-1]
        elif len(base) == 2:
            idx["leer"], idx["unguelt"] = base[-2], base[-1]
        elif len(base) == 1:
            idx["unguelt"] = base[-1]
    return idx

def is_total_row(name):
    n = (name or "").strip()
    return n.startswith("Total ") or n in ("Total Kanton", "Total Auslandschweizer (AS)")

def normalize_wahllokal(name):
    return (name or "").strip()

def to_float_ratio(numer, denom):
    if pd.isna(numer) or pd.isna(denom) or denom == 0:
        return pd.NA
    return float(numer) / float(denom)

def parse_stimmberechtigte(lines):
    rows, start = [], None
    for i, ln in enumerate(lines):
        if "Stimmberechtigte" in ln:
            start = i + 1
            break
    if start is None:
        return pd.DataFrame(columns=["Gemein_Name","Stimmber_Anz","Stimmber_Anz_M","Stimmber_Anz_F"])

    i = start
    while i < len(lines):
        row = [c.strip().strip('"') for c in lines[i].split(",")]
        if not any(row): break
        if row[0].lower().startswith("stimmbeteiligung"): break
        name = GEMEIN_NAME_FIX.get(row[0], row[0])
        tot = clean_num(row[1] if len(row)>1 else None)
        m   = clean_num(row[2] if len(row)>2 else None)
        f   = clean_num(row[3] if len(row)>3 else None)
        if name: rows.append([name, tot, m, f])
        i += 1

    df = pd.DataFrame(rows, columns=["Gemein_Name","Stimmber_Anz","Stimmber_Anz_M","Stimmber_Anz_F"])
    if not df.empty:
        df = df[df["Gemein_Name"].isin(GEMEIN_IDS.keys())]
    return df

def combine_on_name(df_old, df_new):
    if df_old is None or df_old.empty: return df_new
    if df_new is None or df_new.empty: return df_old
    a = df_old.set_index("Gemein_Name")
    b = df_new.set_index("Gemein_Name")
    return b.combine_first(a).reset_index()

def parse_kennzahlen_block(lines):
    hdr, hdr_idx = None, None
    for i, ln in enumerate(lines):
        toks = tokenize_csv_line(ln)
        joined = " ".join(toks).lower()
        if ("durchschnitt" in joined and "stimmbeteilig" in joined) or ("brieflich" in joined) or ("elektron" in joined):
            hdr, hdr_idx = toks, i
            break
    if hdr is None:
        return pd.DataFrame(columns=["Gemein_Name","Durchschn_Stimmbet_pro_Abst_Art","Durchschn_Briefl_Ant_pro_Abst_Art","Anz_Elektr_pro_Abst_Art"])

    def idx_has(*words):
        for j, c in enumerate(hdr):
            cell = (c or "").lower()
            if all(w in cell for w in words): return j
        return None

    i_stb = idx_has("durchschnitt", "stimmbeteilig")
    i_brf = idx_has("brieflich")
    i_ele = idx_has("elektron")

    rows, i = [], hdr_idx + 1
    while i < len(lines):
        toks = tokenize_csv_line(lines[i])
        if not any(toks): break
        name = GEMEIN_NAME_FIX.get((toks[0] or "").strip(), (toks[0] or "").strip())
        vals = {}
        if i_stb is not None and len(toks) > i_stb:
            v = clean_numeric(toks[i_stb]);  v = (v/100.0 if (not pd.isna(v) and v > 1) else v)
            vals["Durchschn_Stimmbet_pro_Abst_Art"] = v
        if i_brf is not None and len(toks) > i_brf:
            v = clean_numeric(toks[i_brf]);  v = (v/100.0 if (not pd.isna(v) and v > 1) else v)
            vals["Durchschn_Briefl_Ant_pro_Abst_Art"] = v
        if i_ele is not None and len(toks) > i_ele:
            vals["Anz_Elektr_pro_Abst_Art"] = clean_num(toks[i_ele])
        rows.append({"Gemein_Name": name, **vals})
        i += 1

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["Gemein_Name"].isin(GEMEIN_IDS.keys())]
        df.loc[df["Gemein_Name"].isin(["Basel","Riehen","Bettingen"]), "Anz_Elektr_pro_Abst_Art"] = pd.NA
    return df

# ====== Main ETL ======
def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # Wahllokale mapping for Details id
    df_wahllok = None
    if os.path.exists(WAHLLOKALE_CSV):
        df_wahllok = pd.read_csv(WAHLLOKALE_CSV, encoding="unicode_escape").rename(columns={"Wahllok_Name": "Wahllok_name"})

    per_date_details = defaultdict(list)
    per_date_kennz = defaultdict(list)
    per_date_stimmber = {}
    per_date_kzextras = defaultdict(list)

    files = sorted(glob.glob(os.path.join(IN_DIR, "*.csv")))
    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        abst_art, abst_date = detect_meta(lines)
        if abst_date is None:
            m = re.search(r"(\d{8})", os.path.basename(path))
            if m:
                abst_date = datetime.strptime(m.group(1), "%Y%m%d").strftime("%Y-%m-%d")
        if abst_art is None:
            abst_art = "national" if ("-eid" in path.lower() or "_eid_" in path.lower()) else "kantonal"

        # merge Stimmberechtigte for this file
        df_stb = parse_stimmberechtigte(lines)
        per_date_stimmber[abst_date] = combine_on_name(per_date_stimmber.get(abst_date), df_stb)

        # Kennzahlen averages / e-voting counts (if present)
        df_kzextra = parse_kennzahlen_block(lines)
        if not df_kzextra.empty:
            per_date_kzextras[abst_date].append(df_kzextra)

        sections = find_sections(lines)
        for sec in sections:
            pre_hdrs, hdr, data_rows, _ = read_block(lines, sec.idx)
            if hdr is None or not data_rows:
                continue

            indices = locate_indices_gv(pre_hdrs, hdr) if sec.is_gv else locate_indices_non_gv(hdr, pre_hdrs)
            # fallback inference if needed
            needed = ["eingel", "leer", "unguelt", "guelt", "ja", "nein"]
            if any(indices.get(k) is None for k in needed):
                inferred = infer_indices_from_layout(data_rows[0])
                for k, v in inferred.items():
                    if indices.get(k) is None:
                        indices[k] = v

            for row in data_rows:
                wl_ix = indices.get("wahllok", 0)
                wahllok = normalize_wahllokal(row[wl_ix] if len(row) > wl_ix else "")
                if (not wahllok
                    or "stimmberechtigte" in wahllok.lower()
                    or "stimmbeteiligung" in wahllok.lower()
                    or "abstimmungs-kennzahlen" in wahllok.lower()
                    or wahllok == '"'):
                    continue

                stimmr = clean_num(row[indices["stimmr"]]) if indices.get("stimmr") is not None and len(row) > indices["stimmr"] else pd.NA
                eingel = clean_num(row[indices["eingel"]]) if indices.get("eingel") is not None and len(row) > indices["eingel"] else pd.NA
                leer   = clean_num(row[indices["leer"]])   if indices.get("leer")   is not None and len(row) > indices["leer"]   else pd.NA
                unguelt= clean_num(row[indices["unguelt"]])if indices.get("unguelt")is not None and len(row) > indices["unguelt"]else pd.NA
                guelt  = clean_num(row[indices["guelt"]])  if indices.get("guelt")  is not None and len(row) > indices["guelt"]  else pd.NA
                ja     = clean_num(row[indices["ja"]])     if indices.get("ja")     is not None and len(row) > indices["ja"]     else pd.NA
                nein   = clean_num(row[indices["nein"]])   if indices.get("nein")   is not None and len(row) > indices["nein"]   else pd.NA

                details_row = {
                    "Wahllok_name": wahllok,
                    "Stimmr_Anz": stimmr,
                    "Eingel_Anz": eingel,
                    "Leer_Anz": leer,
                    "Unguelt_Anz": unguelt,
                    "Guelt_Anz": guelt,
                    "Ja_Anz": ja,
                    "Nein_Anz": nein,
                    "Abst_Titel": sec.title,
                    "Abst_Art": abst_art,
                    "Abst_Datum": abst_date,
                    "Result_Art": "Schlussresultat",
                    "abst_typ": "Abstimmung ohne Gegenvorschlag / Stichfrage",
                    "has_geg": sec.is_gv,
                }

                kennz_target_name = GEMEIN_NAME_FIX.get(wahllok, None)

                if sec.is_gv:
                    def get(ix): return clean_num(row[ix]) if ix is not None and len(row) > ix else pd.NA
                    details_row["abst_typ"] = "Initiative mit Gegenvorschlag und Stichfrage"
                    details_row.update({
                        "Gege_Ja_Anz": get(indices.get("gege_ja")),
                        "Gege_Nein_Anz": get(indices.get("gege_nein")),
                        "Sti_Initiative_Anz": get(indices.get("sti_init")),
                        "Sti_Gegenvorschlag_Anz": get(indices.get("sti_gege")),
                        "Init_OGA_Anz": get(indices.get("init_oga")),
                        "Gege_OGA_Anz": get(indices.get("gege_oga")),
                        "Sti_OGA_Anz": get(indices.get("sti_oga")),
                    })
                    details_row["anteil_ja_stimmen"] = to_float_ratio(ja, nz(ja)+nz(nein))
                    details_row["gege_anteil_ja_Stimmen"] = to_float_ratio(details_row["Gege_Ja_Anz"], nz(details_row["Gege_Ja_Anz"])+nz(details_row["Gege_Nein_Anz"]))
                    details_row["sti_anteil_init_stimmen"] = to_float_ratio(details_row["Sti_Initiative_Anz"], nz(details_row["Sti_Initiative_Anz"])+nz(details_row["Sti_Gegenvorschlag_Anz"]))
                else:
                    details_row["anteil_ja_stimmen"] = to_float_ratio(details_row["Ja_Anz"], details_row["Guelt_Anz"])

                per_date_details[abst_date].append(details_row)

                if kennz_target_name is not None and is_total_row(wahllok):
                    kennz_row = {
                        "Gemein_Name": kennz_target_name,
                        "Stimmr_Anz": stimmr,
                        "Eingel_Anz": eingel,
                        "Leer_Anz": leer,
                        "Unguelt_Anz": unguelt,
                        "Guelt_Anz": guelt,
                        "Ja_Anz": ja,
                        "Nein_Anz": nein,
                        "Abst_Titel": sec.title,
                        "Abst_Art": abst_art,
                        "Abst_Datum": abst_date,
                        "Result_Art": "Schlussresultat",
                        "abst_typ": details_row["abst_typ"],
                        "has_geg": sec.is_gv,
                    }
                    if sec.is_gv:
                        kennz_row.update({
                            "Gege_Ja_Anz": details_row.get("Gege_Ja_Anz"),
                            "Gege_Nein_Anz": details_row.get("Gege_Nein_Anz"),
                            "Sti_Initiative_Anz": details_row.get("Sti_Initiative_Anz"),
                            "Sti_Gegenvorschlag_Anz": details_row.get("Sti_Gegenvorschlag_Anz"),
                            "Init_OGA_Anz": details_row.get("Init_OGA_Anz"),
                            "Gege_OGA_Anz": details_row.get("Gege_OGA_Anz"),
                            "Sti_OGA_Anz": details_row.get("Sti_OGA_Anz"),
                            "gege_anteil_ja_Stimmen": details_row.get("gege_anteil_ja_Stimmen"),
                            "sti_anteil_init_stimmen": details_row.get("sti_anteil_init_stimmen"),
                        })
                        kennz_row["anteil_ja_stimmen"] = to_float_ratio(ja, nz(ja)+nz(nein))
                    else:
                        kennz_row["anteil_ja_stimmen"] = to_float_ratio(ja, guelt)

                    per_date_kennz[abst_date].append(kennz_row)

    # ===== Build & write outputs per date =====
    for date_key in sorted(per_date_details.keys()):
        df_det = pd.DataFrame(per_date_details[date_key])
        df_ken = pd.DataFrame(per_date_kennz.get(date_key, []))

        # Assign Abst_ID per date: national 1..N, cantonal continue
        def assign_ids(df):
            if df.empty: return df
            tuples = list(dict.fromkeys(zip(df["Abst_Art"], df["Abst_Titel"])))
            nat_titles = [t for t in tuples if t[0] == "national"]
            kan_titles = [t for t in tuples if t[0] == "kantonal"]
            id_map, nid = {}, 1
            for _, title in nat_titles:
                id_map[("national", title)] = nid; nid += 1
            cid = nid
            for _, title in kan_titles:
                id_map[("kantonal", title)] = cid; cid += 1
            df["Abst_ID"] = df.apply(lambda r: id_map[(r["Abst_Art"], r["Abst_Titel"])], axis=1)
            return df

        # Drop totals from Details
        drop_names = ["Total Basel","Total Riehen","Total Bettingen","Total Auslandschweizer (AS)","Total Kanton"]
        if not df_det.empty:
            df_det = df_det[~df_det["Wahllok_name"].isin(drop_names)]

        df_det = assign_ids(df_det)
        if not df_ken.empty:
            df_ken = assign_ids(df_ken)

        if not df_det.empty:
            df_det["Abst_Titel"] = df_det.apply(lambda r: format_title(r["Abst_Titel"], r.get("has_geg", False)), axis=1)
            df_det["Abst_ID_Titel"] = df_det["Abst_ID"].astype(str) + ": " + df_det["Abst_Titel"]
        if not df_ken.empty:
            df_ken["Abst_Titel"] = df_ken.apply(lambda r: format_title(r["Abst_Titel"], r.get("has_geg", False)), axis=1)
            df_ken["Abst_ID_Titel"] = df_ken["Abst_ID"].astype(str) + ": " + df_ken["Abst_Titel"]

        for df in (df_det, df_ken):
            if not df.empty and "has_geg" in df.columns:
                df.drop(columns=["has_geg"], inplace=True)

        # Merge Wahllok IDs
        if df_wahllok is not None and not df_det.empty:
            df_det = df_det.merge(df_wahllok, on="Wahllok_name", how="left")
            df_det["id"] = (
                df_det["Abst_Datum"] + "_" +
                df_det["Abst_ID"].astype(int).astype(str).str.zfill(2) + "_" +
                df_det["wahllok_id"].astype("Int64").astype(str)
            )

        # Kennzahlen enrichments
        if not df_ken.empty:
            df_ken["Gemein_Name"] = df_ken["Gemein_Name"].map(lambda x: GEMEIN_NAME_FIX.get(x, x))
            df_ken["Gemein_ID"] = df_ken["Gemein_Name"].map(GEMEIN_IDS).astype("Int64")

            df_stb = per_date_stimmber.get(date_key)
            if df_stb is not None and not df_stb.empty:
                df_ken = df_ken.merge(df_stb, on="Gemein_Name", how="left")

            kz_list = per_date_kzextras.get(date_key, [])
            if kz_list:
                df_extra = pd.concat(kz_list).groupby("Gemein_Name", as_index=False).first()
                df_ken = df_ken.merge(df_extra, on="Gemein_Name", how="left")

            # 1) Durchschnittliche Stimmbeteiligung = Stimmr_Anz / Stimmber_Anz
            if {"Stimmr_Anz","Stimmber_Anz"}.issubset(df_ken.columns):
                stimmbet = (df_ken["Stimmr_Anz"] / df_ken["Stimmber_Anz"]).where(df_ken["Stimmber_Anz"].notna())
                if "Durchschn_Stimmbet_pro_Abst_Art" in df_ken.columns:
                    df_ken["Durchschn_Stimmbet_pro_Abst_Art"] = df_ken["Durchschn_Stimmbet_pro_Abst_Art"].fillna(stimmbet)
                else:
                    df_ken["Durchschn_Stimmbet_pro_Abst_Art"] = stimmbet

            # 2) Anteil brieflich Stimmender per Gemeinde
            #    Basel/Riehen/Bettingen: (brieflich Stimmende in Details) / (Gemeinde Stimmr_Anz in Kennzahlen)
            #    Basel-Stadt: sum of all three brieflich rows / Basel-Stadt Stimmr_Anz
            if not df_det.empty:
                brief_map = {
                    "Basel": ["Basel brieflich Stimmende"],
                    "Riehen": ["Riehen brieflich Stimmende"],
                    "Bettingen": ["Bettingen brieflich Stimmende"],
                    "Basel-Stadt": ["Basel brieflich Stimmende", "Riehen brieflich Stimmende", "Bettingen brieflich Stimmende"],
                    "Auslandschweizer/-innen": ["Brieflich Stimmende AS"]
                }
                parts = []
                for gname, rows in brief_map.items():
                    tmp = (
                        df_det[df_det["Wahllok_name"].isin(rows)]
                        .groupby("Abst_ID", as_index=False)["Stimmr_Anz"].sum()
                        .rename(columns={"Stimmr_Anz": "briefl_stimmr"})
                    )
                    tmp["Gemein_Name"] = gname
                    parts.append(tmp)
                df_briefl = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["Abst_ID","Gemein_Name","briefl_stimmr"])

                if not df_briefl.empty:
                    df_ken = df_ken.merge(df_briefl, on=["Abst_ID","Gemein_Name"], how="left")
                    brf_ratio = (df_ken["briefl_stimmr"] / df_ken["Stimmr_Anz"]).where(df_ken["Stimmr_Anz"].notna())
                    if "Durchschn_Briefl_Ant_pro_Abst_Art" in df_ken.columns:
                        df_ken["Durchschn_Briefl_Ant_pro_Abst_Art"] = df_ken["Durchschn_Briefl_Ant_pro_Abst_Art"].fillna(brf_ratio)
                    else:
                        df_ken["Durchschn_Briefl_Ant_pro_Abst_Art"] = brf_ratio
                    df_ken.drop(columns=["briefl_stimmr"], inplace=True)

            # 3) Anzahl elektronisch Stimmender = read from Details row "Elektronisch Stimmende AS"
            ele = (
                df_det[df_det["Wahllok_name"] == "Elektronisch Stimmende AS"]
                .groupby("Abst_ID", as_index=False)["Stimmr_Anz"].sum()
                .rename(columns={"Stimmr_Anz": "Anz_Elektr_pro_Abst_Art"})
            )
            if not ele.empty:
                df_ken = df_ken.merge(ele, on="Abst_ID", how="left", suffixes=("", "_elecalc"))
                # apply only to Auslandschweizer/-innen; NA for Basel/Riehen/Bettingen
                mask_as = df_ken["Gemein_Name"].eq("Auslandschweizer/-innen")
                calc = df_ken["Anz_Elektr_pro_Abst_Art_elecalc"].where(mask_as)
                if "Anz_Elektr_pro_Abst_Art" in df_ken.columns:
                    df_ken.loc[mask_as, "Anz_Elektr_pro_Abst_Art"] = df_ken.loc[mask_as, "Anz_Elektr_pro_Abst_Art"].fillna(df_ken.loc[mask_as, "Anz_Elektr_pro_Abst_Art_elecalc"])
                    df_ken.loc[~mask_as, "Anz_Elektr_pro_Abst_Art"] = pd.NA
                else:
                    df_ken["Anz_Elektr_pro_Abst_Art"] = calc
                df_ken.drop(columns=["Anz_Elektr_pro_Abst_Art_elecalc"], inplace=True)

            df_ken["id"] = (
                df_ken["Abst_Datum"] + "_" +
                df_ken["Abst_ID"].astype(int).astype(str).str.zfill(2) + "_" +
                df_ken["Gemein_ID"].astype("Int64").astype(str).str.zfill(2)
            )

        # Ensure ratios present
        if not df_det.empty and "anteil_ja_stimmen" not in df_det.columns:
            df_det["anteil_ja_stimmen"] = df_det["Ja_Anz"] / df_det["Guelt_Anz"]
        if not df_ken.empty and "anteil_ja_stimmen" not in df_ken.columns:
            df_ken["anteil_ja_stimmen"] = df_ken["Ja_Anz"] / df_ken["Guelt_Anz"]

        # Order columns
        det_cols_base = [
            "Wahllok_name","Stimmr_Anz","Eingel_Anz","Leer_Anz","Unguelt_Anz","Guelt_Anz","Ja_Anz","Nein_Anz",
            "Abst_Titel","Abst_Art","Abst_Datum","Result_Art","Abst_ID","anteil_ja_stimmen","abst_typ",
        ]
        det_cols_gv = [
            "Gege_Ja_Anz","Gege_Nein_Anz","Sti_Initiative_Anz","Sti_Gegenvorschlag_Anz",
            "gege_anteil_ja_Stimmen","sti_anteil_init_stimmen","Init_OGA_Anz","Gege_OGA_Anz","Sti_OGA_Anz",
        ]
        det_cols_tail = ["Abst_ID_Titel","wahllok_id","gemein_id","gemein_name","id","Gemein_ID"]
        use_det_cols = [c for c in det_cols_base if c in df_det.columns] + \
                       [c for c in det_cols_gv if c in df_det.columns] + \
                       [c for c in det_cols_tail if c in df_det.columns]
        df_det = df_det[use_det_cols]

        ken_cols_base = [
            "Gemein_Name","Stimmr_Anz","Eingel_Anz","Leer_Anz","Unguelt_Anz","Guelt_Anz","Ja_Anz","Nein_Anz",
            "Abst_Titel","Abst_Art","Abst_Datum","Result_Art","Abst_ID","anteil_ja_stimmen","Gemein_ID",
            "Durchschn_Stimmbet_pro_Abst_Art","Durchschn_Briefl_Ant_pro_Abst_Art","Anz_Elektr_pro_Abst_Art",
            "Stimmber_Anz","Stimmber_Anz_M","Stimmber_Anz_F","abst_typ",
        ]
        ken_cols_gv = [
            "Gege_Ja_Anz","Gege_Nein_Anz","Sti_Initiative_Anz","Sti_Gegenvorschlag_Anz",
            "gege_anteil_ja_Stimmen","sti_anteil_init_stimmen","Init_OGA_Anz","Gege_OGA_Anz","Sti_OGA_Anz",
        ]
        ken_cols_tail = ["Abst_ID_Titel","id"]
        use_ken_cols = [c for c in ken_cols_base if c in df_ken.columns] + \
                       [c for c in ken_cols_gv if c in df_ken.columns] + ken_cols_tail
        df_ken = df_ken[use_ken_cols] if not df_ken.empty else df_ken

        out_det = os.path.join(OUT_DIR, f"Abstimmungen_Details_{date_key}.csv")
        out_ken = os.path.join(OUT_DIR, f"Abstimmungen_{date_key}.csv")
        df_det.to_csv(out_det, index=False)
        if not df_ken.empty:
            df_ken.to_csv(out_ken, index=False)
        else:
            pd.DataFrame(columns=use_ken_cols).to_csv(out_ken, index=False)
        print(f"Wrote:\n  {out_det}\n  {out_ken}")

if __name__ == "__main__":
    main()
