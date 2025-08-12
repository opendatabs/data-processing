import glob
import io
import json
import logging
from pathlib import Path

import common
import numpy as np
import pandas as pd
import pdfplumber

pd.set_option("future.no_silent_downcasting", True)
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logging.getLogger("pdfplumber").setLevel(logging.WARNING)


def _pdfs_with_year(folder_glob: str):
    """
    Yield (year:int, path:str) for PDFs whose basename starts with 4-digit year.
    Example filenames: '2023 Schulexterne Tagesstrukturen.pdf'
    """
    for p in sorted(glob.glob(folder_glob)):
        name = Path(p).name
        try:
            year = int(name[:4])
        except ValueError:
            continue  # skip files not starting with a year
        yield year, p


def _assert_close(name: str, got, want, year: int, atol: float = 1e-6):
    g = 0 if pd.isna(got) else float(got)
    w = 0 if pd.isna(want) else float(want)
    if not np.isclose(g, w, atol=atol, rtol=0):
        raise ValueError(f"Mismatch {name} for Jahr {year}: sheet={g} vs agg={w}")


def _cell0(df_row_like, col):
    """Return a single numeric value from the first row's column; NaN -> 0.0"""
    v = pd.to_numeric(df_row_like[col].iloc[0], errors="coerce")
    return 0.0 if pd.isna(v) else float(v)


def main():
    df_standorte = get_standorte()
    df_schulexterne_nach_gemeinde = process_schulexterne_tagesstrukturen()
    df_schuleigene_nach_gemeinde = process_schuleigene_tagesstrukturen(df_standorte)
    df_tagesferien = process_tagesferien()

    process_anzahl_plaetze(df_schulexterne_nach_gemeinde, df_schuleigene_nach_gemeinde, df_tagesferien)

    process_oeffentliche_ausgaben()

    process_anzahl_kinder(df_schulexterne_nach_gemeinde, df_schuleigene_nach_gemeinde, df_tagesferien)


def get_standorte():
    tagesstrukturen_basel = get_standorte_basel()
    tagesstrukturen_riehen, tagesstrukuren_bettingen = get_standorte_riehen_bettingen()
    # Create a DataFrame for all locations
    return pd.DataFrame(
        {
            "standort": tagesstrukturen_basel + tagesstrukturen_riehen + tagesstrukuren_bettingen,
            "gemeinde": ["Basel"] * len(tagesstrukturen_basel)
            + ["Riehen"] * len(tagesstrukturen_riehen)
            + ["Bettingen"] * len(tagesstrukuren_bettingen),
        }
    )


def get_standorte_basel():
    url_to_standorte = "https://data.bs.ch/explore/dataset/100029/download/"
    params = {
        "format": "csv",
        "refine.sc_schultyp": "Tagesstruktur",
    }
    r = common.requests_get(url_to_standorte, params=params)
    r.raise_for_status()
    df_basel = pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")
    df_basel["sc_schulstandort"] = df_basel["sc_schulstandort"].str.replace("TS ", "", regex=False)
    return df_basel["sc_schulstandort"].unique().tolist()


def get_standorte_riehen_bettingen():
    url_to_standorte = "https://data.bs.ch/explore/dataset/100030/download/"
    params = {
        "format": "csv",
        "refine.typ": "Tagesstruktur",
    }
    r = common.requests_get(url_to_standorte, params=params)
    r.raise_for_status()
    df_riehen_bettingen = pd.read_csv(io.StringIO(r.content.decode("utf-8")), sep=";")
    df_riehen_bettingen["standort"] = df_riehen_bettingen["standort"].str.replace("Primarstufe ", "", regex=False)
    standorte_riehen = df_riehen_bettingen[df_riehen_bettingen["ort"] == "Riehen"]["standort"].unique().tolist()
    standorte_bettingen = df_riehen_bettingen[df_riehen_bettingen["ort"] == "Bettingen"]["standort"].unique().tolist()
    return standorte_riehen, standorte_bettingen


def process_schulexterne_tagesstrukturen():
    frames = []
    for jahr, pdf_path in _pdfs_with_year("data_orig/schulexterne/*.pdf"):
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            table = page.extract_table()
            df = pd.DataFrame(table[1:], columns=table[0])

        df.columns = [
            "number",
            "mittagstisch",
            "anz_pl_pro_tag_mm",
            "anz_pl_pro_tag_nm1",
            "anz_pl_pro_tag_nm2l",
            "anz_pl_pro_tag_nm2k",
            "bel_stichwoche_mm",
            "bel_stichwoche_auslastung_mm",
            "bel_stichwoche_nm1",
            "bel_stichwoche_nm2l",
            "bel_stichwoche_nm2k",
            "tot_angm_anzahl",
            "tot_angm_knaben",
            "tot_angm_maedchen",
            "tot_angm_KG",
            "tot_angm_PS",
        ]
        df = df.iloc[2:-2].copy()
        df.reset_index(drop=True, inplace=True)
        df = df.drop(columns=["number"])
        df["mittagstisch"] = df["mittagstisch"].str.replace(r"\s*[13]$", "", regex=True)
        df["jahr"] = jahr
        frames.append(df)

    if not frames:
        raise ValueError("Keine schulexterne PDFs gefunden in data_orig/schulexterne/")

    df_schulexterne = pd.concat(frames, ignore_index=True)

    with open("data_orig/mittagstisch_gemeinde_mapping.json", "r", encoding="utf-8") as f:
        mittagstisch_gemeinde_mapping = json.load(f)
    df_schulexterne["gemeinde"] = df_schulexterne["mittagstisch"].map(mittagstisch_gemeinde_mapping)

    num_cols = [
        "anz_pl_pro_tag_mm",
        "anz_pl_pro_tag_nm1",
        "anz_pl_pro_tag_nm2l",
        "anz_pl_pro_tag_nm2k",
        "bel_stichwoche_mm",
        "bel_stichwoche_nm1",
        "bel_stichwoche_nm2l",
        "bel_stichwoche_nm2k",
        "tot_angm_anzahl",
        "tot_angm_knaben",
        "tot_angm_maedchen",
        "tot_angm_KG",
        "tot_angm_PS",
    ]
    for col in num_cols:
        df_schulexterne[col] = clean_numeric(df_schulexterne[col])
    df_schulexterne["bel_stichwoche_auslastung_mm"] = clean_percent(df_schulexterne["bel_stichwoche_auslastung_mm"])

    df_schulexterne_agg = df_schulexterne.groupby(["gemeinde", "jahr"], as_index=False).agg(
        {
            "anz_pl_pro_tag_mm": "sum",
            "anz_pl_pro_tag_nm1": "sum",
            "anz_pl_pro_tag_nm2l": "sum",
            "anz_pl_pro_tag_nm2k": "sum",
            "bel_stichwoche_mm": "sum",
            "bel_stichwoche_auslastung_mm": "mean",
            "bel_stichwoche_nm1": "sum",
            "bel_stichwoche_nm2l": "sum",
            "bel_stichwoche_nm2k": "sum",
            "tot_angm_anzahl": "sum",
            "tot_angm_knaben": "sum",
            "tot_angm_maedchen": "sum",
            "tot_angm_KG": "sum",
            "tot_angm_PS": "sum",
        }
    )

    df_schulexterne.to_csv("data/schulexterne_tagesstrukturen.csv", index=False)
    df_schulexterne_agg.to_csv("data/100458_schulexterne_tagesstrukturen_aggregiert.csv", index=False)
    return df_schulexterne_agg


def process_schuleigene_tagesstrukturen(df_standorte):
    frames = []
    for jahr, pdf_path in _pdfs_with_year("data_orig/schuleigene/*.pdf"):
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[0]
            table = page.extract_table()
            df = pd.DataFrame(table[1:], columns=table[0])

        df.columns = [
            "stufe",
            "schule",
            "anz_pl_pro_tag_fruehhort",
            "anz_pl_pro_tag_mm",
            "anz_pl_pro_tag_nm",
            "bel_stichwoche_fruehhort",
            "bel_stichwoche_mm",
            "bel_stichwoche_auslastung_mm",
            "bel_stichwoche_nm1",
            "bel_stichwoche_nm2l",
            "bel_stichwoche_nm2k",
            "tot_angm_anzahl",
            "tot_angm_knaben",
            "tot_angm_maedchen",
            "tot_angm_KG",
            "tot_angm_PS",
            "wochenbel_1tag",
            "wochenbel_2tage",
            "wochenbel_3tage",
            "wochenbel_4tage",
            "wochenbel_5tage",
        ]
        df = df.iloc[2:-2].copy()
        df.reset_index(drop=True, inplace=True)
        df["schule"] = df["schule"].str.replace(r"\*$", "", regex=True)
        df = df.merge(df_standorte, left_on="schule", right_on="standort", how="left").drop(columns=["standort"])
        df = df.drop(columns=["stufe"])
        df["jahr"] = jahr
        frames.append(df)

    if not frames:
        raise ValueError("Keine schuleigene PDFs gefunden in data_orig/schuleigene/")

    df_schuleigene = pd.concat(frames, ignore_index=True)

    num_cols = [
        "anz_pl_pro_tag_fruehhort",
        "anz_pl_pro_tag_mm",
        "anz_pl_pro_tag_nm",
        "bel_stichwoche_fruehhort",
        "bel_stichwoche_mm",
        "bel_stichwoche_nm1",
        "bel_stichwoche_nm2l",
        "bel_stichwoche_nm2k",
        "tot_angm_anzahl",
        "tot_angm_knaben",
        "tot_angm_maedchen",
        "tot_angm_KG",
        "tot_angm_PS",
        "wochenbel_1tag",
        "wochenbel_2tage",
        "wochenbel_3tage",
        "wochenbel_4tage",
        "wochenbel_5tage",
    ]
    for col in num_cols:
        df_schuleigene[col] = clean_numeric(df_schuleigene[col])

    df_schuleigene["bel_stichwoche_auslastung_mm"] = clean_percent(df_schuleigene["bel_stichwoche_auslastung_mm"])

    df_schuleigene_agg = df_schuleigene.groupby(["gemeinde", "jahr"], as_index=False).agg(
        {
            "anz_pl_pro_tag_fruehhort": "sum",
            "anz_pl_pro_tag_mm": "sum",
            "anz_pl_pro_tag_nm": "sum",
            "bel_stichwoche_fruehhort": "sum",
            "bel_stichwoche_mm": "sum",
            "bel_stichwoche_auslastung_mm": "mean",
            "bel_stichwoche_nm1": "sum",
            "bel_stichwoche_nm2l": "sum",
            "bel_stichwoche_nm2k": "sum",
            "tot_angm_anzahl": "sum",
            "tot_angm_knaben": "sum",
            "tot_angm_maedchen": "sum",
            "tot_angm_KG": "sum",
            "tot_angm_PS": "sum",
            "wochenbel_1tag": "sum",
            "wochenbel_2tage": "sum",
            "wochenbel_3tage": "sum",
            "wochenbel_4tage": "sum",
            "wochenbel_5tage": "sum",
        }
    )

    df_schuleigene.to_csv("data/100453_schuleigene_tagesstrukturen.csv", index=False)
    df_schuleigene_agg.to_csv("data/schuleigene_tagesstrukturen_aggregiert.csv", index=False)
    return df_schuleigene_agg


def process_anzahl_plaetze(df_schulexterne, df_schuleigene, df_tagesferien):
    df_plaetze = pd.read_excel("data_orig/t13-2-40.xlsx", sheet_name="Plätze", usecols="B:C,E:F,H:J")
    df_plaetze.columns = [
        "jahr",
        "fruehhort",
        "schuleigene_module_mittag",
        "schuleigene_module_nachmittag",
        "schulexterne_module_mittag",
        "schulexterne_module_nachmittag",
        "tagesferien",
    ]
    df_plaetze = df_plaetze.iloc[11:-2].reset_index(drop=True).replace(["… ", "…"], pd.NA)

    # Aggregations from schuleigene/schulexterne
    ig = df_schuleigene.groupby("jahr", as_index=False).agg(
        fruehhort=("anz_pl_pro_tag_fruehhort", "sum"),
        schuleigene_module_mittag=("anz_pl_pro_tag_mm", "sum"),
        schuleigene_module_nachmittag=("anz_pl_pro_tag_nm", "sum"),
    )
    ex = df_schulexterne.groupby("jahr", as_index=False).agg(
        schulexterne_module_mittag=("anz_pl_pro_tag_mm", "sum"),
        schulexterne_module_nachmittag=("anz_pl_pro_tag_nm1", "sum"),
    )

    # Tagesferien aggregation
    tf = df_tagesferien.copy()
    for c in ["anzahl_angebotene_wochen"]:
        tf[c] = pd.to_numeric(tf[c], errors="coerce")
    tf_agg = tf.groupby("jahr", as_index=False).agg(
        tagesferien_wochen=("anzahl_angebotene_wochen", "sum"),
    )

    years = sorted(set(df_schulexterne["jahr"]).union(df_schuleigene["jahr"]))
    for y in years:
        row = df_plaetze.loc[df_plaetze["jahr"] == y]
        if row.empty:
            continue
        total_ig = ig.loc[ig["jahr"] == y].drop(columns=["jahr"]).sum(numeric_only=True)
        total_ex = ex.loc[ex["jahr"] == y].drop(columns=["jahr"]).sum(numeric_only=True)

        _assert_close("fruehhort", _cell0(row, "fruehhort"), total_ig["fruehhort"], y)
        _assert_close(
            "schuleigene_module_mittag",
            _cell0(row, "schuleigene_module_mittag"),
            total_ig["schuleigene_module_mittag"],
            y,
        )
        _assert_close(
            "schuleigene_module_nachmittag",
            _cell0(row, "schuleigene_module_nachmittag"),
            total_ig["schuleigene_module_nachmittag"],
            y,
        )
        _assert_close(
            "schulexterne_module_mittag",
            _cell0(row, "schulexterne_module_mittag"),
            total_ex["schulexterne_module_mittag"],
            y,
        )
        _assert_close(
            "schulexterne_module_nachmittag",
            _cell0(row, "schulexterne_module_nachmittag"),
            total_ex["schulexterne_module_nachmittag"],
            y,
        )

        # Tagesferien equalness: sheet vs sum(anzahl_angebotene_wochen)
        tf_row = tf_agg.loc[tf_agg["jahr"] == y]
        if not tf_row.empty:
            _assert_close(
                "tagesferien (Plätze vs Tagesferien-Wochen)",
                _cell0(row, "tagesferien"),
                float(tf_row["tagesferien_wochen"].iloc[0] or 0),
                y,
            )

    df_plaetze.to_csv("data/100454_anzahl_plaetze.csv", index=False)


def process_oeffentliche_ausgaben():
    df_ausgaben_basel = pd.read_excel("data_orig/t13-2-40.xlsx", sheet_name="Ausgaben", usecols="B:F")
    df_ausgaben_basel.columns = [
        "jahr",
        "schulexterne_module",
        "schuleigene_module",
        "tagesferien",
        "ferienbetreuung",
    ]
    df_ausgaben_basel["gemeinde"] = "Basel"
    df_ausgaben_basel = df_ausgaben_basel.iloc[9:-2].copy()
    df_ausgaben_basel.reset_index(drop=True, inplace=True)
    df_ausgaben_basel = df_ausgaben_basel.replace(["… ", "…"], pd.NA).infer_objects(copy=False)
    df_ausgaben_riehen_bettingen = pd.read_excel("data_orig/t13-2-40.xlsx", sheet_name="Ausgaben", usecols="B,H:J")
    df_ausgaben_riehen_bettingen.columns = [
        "jahr",
        "schulexterne_module",
        "schuleigene_module",
        "tagesferien",
    ]
    df_ausgaben_riehen_bettingen["gemeinde"] = "Riehen und Bettingen"
    df_ausgaben_riehen_bettingen = df_ausgaben_riehen_bettingen.iloc[9:-2].copy()
    df_ausgaben_riehen_bettingen.reset_index(drop=True, inplace=True)
    df_ausgaben_riehen_bettingen = df_ausgaben_riehen_bettingen.replace(["… ", "…"], pd.NA).infer_objects(copy=False)
    # Concatenate the two DataFrames
    df_ausgaben = pd.concat([df_ausgaben_basel, df_ausgaben_riehen_bettingen], ignore_index=True)
    df_ausgaben = df_ausgaben.dropna(
        subset=["schulexterne_module", "schuleigene_module", "tagesferien", "ferienbetreuung"], how="all"
    )
    df_ausgaben.to_csv("data/100455_oeffentliche_ausgaben.csv", index=False)


def process_tagesferien():
    df_tagesferien_stadt_basel = pd.read_excel("data_orig/Gesamtübersicht Tagesferien.xlsx", usecols="A:B, E, H, L:M")
    df_tagesferien_stadt_basel.columns = [
        "jahr",
        "anzahl_angebotene_wochen",
        "anzahl_teilnehmende_sus",
        "durschnittliche_teilnahme",
        "ferienbetreuung_total_buchungen",
        "ferienbetreuung_durchschnittlich_teilnehmende",
    ]
    df_tagesferien_stadt_basel["gemeinde"] = "Basel"
    df_tagesferien_stadt_basel = df_tagesferien_stadt_basel.iloc[4:-16].copy()
    df_tagesferien_stadt_basel.reset_index(drop=True, inplace=True)

    df_tagesferien_riehen_bettingen = pd.read_excel("data_orig/Gesamtübersicht Tagesferien.xlsx", usecols="A, C, F, I")
    df_tagesferien_riehen_bettingen.columns = [
        "jahr",
        "anzahl_angebotene_wochen",
        "anzahl_teilnehmende_sus",
        "durschnittliche_teilnahme",
    ]
    df_tagesferien_riehen_bettingen["gemeinde"] = "Riehen und Bettingen"
    df_tagesferien_riehen_bettingen = df_tagesferien_riehen_bettingen.iloc[4:-16].copy()
    df_tagesferien_riehen_bettingen.reset_index(drop=True, inplace=True)

    # Concatenate the two DataFrames
    df_tagesferien = pd.concat([df_tagesferien_stadt_basel, df_tagesferien_riehen_bettingen], ignore_index=True)

    with open("data_orig/jahr_ferienwochen_mapping.json", "r", encoding="utf-8") as f:
        jahr_ferienwochen_mapping = json.load(f)
        # Convert keys to integers
        jahr_ferienwochen_mapping = {int(k): v for k, v in jahr_ferienwochen_mapping.items()}

    df_tagesferien["anzahl_ferienwochen"] = df_tagesferien["jahr"].map(jahr_ferienwochen_mapping)
    df_tagesferien.to_csv("data/100456_tagesferien.csv", index=False)

    return df_tagesferien


def process_anzahl_kinder(df_schulexterne, df_schuleigene, df_tagesferien):
    df_kinder = pd.read_excel("data_orig/t13-2-40.xlsx", sheet_name="Kinder", usecols="B:C,E:G,I:M")
    df_kinder.columns = [
        "jahr",
        "fruehhort",
        "schuleigene_module_mittag",
        "schuleigene_module_nachmittag1",
        "schuleigene_module_nachmittag2",
        "schulexterne_module_mittag",
        "schulexterne_module_nachmittag1",
        "schulexterne_module_nachmittag2",
        "tagesferien",
        "ferienbetreuung",
    ]
    df_kinder = df_kinder.iloc[9:-2].reset_index(drop=True).replace(["… ", "…"], pd.NA)

    ig = df_schuleigene.groupby("jahr", as_index=False).agg(
        fruehhort=("bel_stichwoche_fruehhort", "sum"),
        schuleigene_module_mittag=("bel_stichwoche_mm", "sum"),
        schuleigene_module_nachmittag=("bel_stichwoche_nm1", "sum"),
        schuleigene_module_nachmittag2l=("bel_stichwoche_nm2l", "sum"),
        schuleigene_module_nachmittag2k=("bel_stichwoche_nm2k", "sum"),
    )
    ex = df_schulexterne.groupby("jahr", as_index=False).agg(
        schulexterne_module_mittag=("bel_stichwoche_mm", "sum"),
        schulexterne_module_nachmittag=("bel_stichwoche_nm1", "sum"),
        schulexterne_module_nachmittag2l=("bel_stichwoche_nm2l", "sum"),
        schulexterne_module_nachmittag2k=("bel_stichwoche_nm2k", "sum"),
    )

    # Tagesferien aggregation
    tf = df_tagesferien.copy()
    for c in ["anzahl_teilnehmende_sus", "ferienbetreuung_total_buchungen"]:
        tf[c] = pd.to_numeric(tf[c], errors="coerce")
    tf_agg = tf.groupby("jahr", as_index=False).agg(
        teilnehmende=("anzahl_teilnehmende_sus", "sum"),
        buchungen=("ferienbetreuung_total_buchungen", "sum"),
    )

    years = sorted(set(df_schulexterne["jahr"]).union(df_schuleigene["jahr"]))
    for y in years:
        row = df_kinder.loc[df_kinder["jahr"] == y]
        if row.empty:
            continue
        total_ig = ig.loc[ig["jahr"] == y].drop(columns=["jahr"]).sum(numeric_only=True)
        total_ex = ex.loc[ex["jahr"] == y].drop(columns=["jahr"]).sum(numeric_only=True)

        _assert_close("fruehhort", _cell0(row, "fruehhort"), total_ig["fruehhort"], y)
        _assert_close(
            "schuleigene_module_mittag",
            _cell0(row, "schuleigene_module_mittag"),
            total_ig["schuleigene_module_mittag"],
            y,
        )

        ig_sheet_sum = _cell0(row, "schuleigene_module_nachmittag1") + _cell0(row, "schuleigene_module_nachmittag2")
        ig_agg_sum = (
            total_ig["schuleigene_module_nachmittag"]
            + total_ig["schuleigene_module_nachmittag2l"]
            + total_ig["schuleigene_module_nachmittag2k"]
        )
        _assert_close("schuleigene_module_nachmittag (1+2)", ig_sheet_sum, ig_agg_sum, y)

        _assert_close(
            "schulexterne_module_mittag",
            _cell0(row, "schulexterne_module_mittag"),
            total_ex["schulexterne_module_mittag"],
            y,
        )

        ex_sheet_sum = _cell0(row, "schulexterne_module_nachmittag1") + _cell0(row, "schulexterne_module_nachmittag2")
        ex_agg_sum = (
            total_ex["schulexterne_module_nachmittag"]
            + total_ex["schulexterne_module_nachmittag2l"]
            + total_ex["schulexterne_module_nachmittag2k"]
        )
        _assert_close("schulexterne_module_nachmittag (1+2)", ex_sheet_sum, ex_agg_sum, y)

        # Tagesferien equalness: Kinder.tagesferien vs sum(teilnehmende),
        # and Kinder.ferienbetreuung vs sum(buchungen)
        tf_row = tf_agg.loc[tf_agg["jahr"] == y]
        if not tf_row.empty:
            _assert_close(
                "tagesferien (Kinder vs Teilnehmende)",
                _cell0(row, "tagesferien"),
                float(tf_row["teilnehmende"].iloc[0] or 0),
                y,
            )
            _assert_close(
                "ferienbetreuung (Kinder vs Buchungen)",
                _cell0(row, "ferienbetreuung"),
                float(tf_row["buchungen"].iloc[0] or 0),
                y,
            )

    df_kinder.to_csv("data/100457_anzahl_kinder.csv", index=False)


def clean_numeric(series: pd.Series) -> pd.Series:
    """
    Clean a Series containing numbers stored as strings.
    Removes spaces, non-breaking spaces, and non-numeric characters
    except signs, dots, and commas. Converts to float.
    """
    return (
        series.astype(str)
        .str.strip()
        .str.replace("\u00a0", " ", regex=False)  # non-breaking space
        .str.replace(",", ".", regex=False)  # Swiss decimals
        .str.replace(r"[^\d.\-]", "", regex=True)  # keep digits, dot, minus
        .apply(pd.to_numeric, errors="coerce")
    )


def clean_percent(series: pd.Series) -> pd.Series:
    """
    Clean a Series containing percentages (e.g. '75%', '  12,5 %').
    Returns values in 0–100 range as float.
    """
    return clean_numeric(series.str.replace("%", "", regex=False))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
