import io
import json

import pdfplumber
import pandas as pd
import common


def main():
    df_standorte = get_standorte()

    process_schuleigene_tagesstrukturen(df_standorte)

    process_schulexterne_tagesstrukturen()

    process_anzahl_plaetze()

    process_oeffentliche_ausgaben()

    process_tagesferien()
    
    process_anzahl_kinder()


def get_standorte():
    tagesstrukturen_basel = get_standorte_basel()
    tagesstrukturen_riehen, tagesstrukuren_bettingen = get_standorte_riehen_bettingen()
    # Create a DataFrame for all locations
    return pd.DataFrame({
        "standort": tagesstrukturen_basel + tagesstrukturen_riehen + tagesstrukuren_bettingen,
        "gemeinde": ["Basel"] * len(tagesstrukturen_basel) +
                    ["Riehen"] * len(tagesstrukturen_riehen) +
                    ["Bettingen"] * len(tagesstrukuren_bettingen)
    })


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
    with pdfplumber.open("data_orig/Schulexterne Tagesstrukturen.pdf") as pdf:
        page = pdf.pages[0]  # adjust page number
        table = page.extract_table()
        df_schulexterne = pd.DataFrame(table[1:], columns=table[0])
    # Rename columns
    df_schulexterne.columns = [
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
    df_schulexterne = df_schulexterne.iloc[2:-2].reset_index(drop=True)
    df_schulexterne = df_schulexterne.drop(columns=["number"])
    df_schulexterne["mittagstisch"] = df_schulexterne["mittagstisch"].str.replace(r"\s*[13]$", "", regex=True)

    # Load the mapping from the JSON file
    with open("data_orig/mittagstisch_gemeinde_mapping.json", "r", encoding="utf-8") as f:
        mittagstisch_gemeinde_mapping = json.load(f)

    df_schulexterne["gemeinde"] = df_schulexterne["mittagstisch"].map(mittagstisch_gemeinde_mapping)
    # Apply numeric cleaning to all numeric columns except percentage
    num_cols = [
        "anz_pl_pro_tag_mm","anz_pl_pro_tag_nm1","anz_pl_pro_tag_nm2l","anz_pl_pro_tag_nm2k",
        "bel_stichwoche_mm","bel_stichwoche_nm1","bel_stichwoche_nm2l","bel_stichwoche_nm2k",
        "tot_angm_anzahl","tot_angm_knaben","tot_angm_maedchen","tot_angm_KG","tot_angm_PS"
    ]
    for col in num_cols:
        df_schulexterne[col] = clean_numeric(df_schulexterne[col])
    # Special handling for percent
    df_schulexterne["bel_stichwoche_auslastung_mm"] = clean_percent(
        df_schulexterne["bel_stichwoche_auslastung_mm"]
    )

    # Aggregate by gemeinde
    df_schulexterne = df_schulexterne.groupby("gemeinde").agg({
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
        "tot_angm_PS": "sum"
    }).reset_index()

    df_schulexterne.to_csv("data/schulexterne_tagesstrukturen.csv", index=False)

def process_schuleigene_tagesstrukturen(df_standorte):
    with pdfplumber.open("data_orig/Tagesstrukturen PS.pdf") as pdf:
        page = pdf.pages[0]  # adjust page number
        table = page.extract_table()
        df_schuleigene = pd.DataFrame(table[1:], columns=table[0])
    # Rename columns
    df_schuleigene.columns = [
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
    df_schuleigene = df_schuleigene.iloc[2:-2].reset_index(drop=True)
    # Remove the * at the end in schule
    df_schuleigene["schule"] = df_schuleigene["schule"].str.replace(r"\*$", "", regex=True)
    # Merge with df_standorte to add gemeinde
    df_schuleigene = df_schuleigene.merge(df_standorte, left_on="schule", right_on="standort", how="left")
    df_schuleigene.to_csv("data/100453_schuleigene_tagesstrukturen.csv", index=False)
    return df_schuleigene


def process_anzahl_plaetze():
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
    df_plaetze = df_plaetze.iloc[11:-2].reset_index(drop=True)
    df_plaetze = df_plaetze.replace("… ", pd.NA)
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
    df_ausgaben_basel["gemeinden"] = "Basel"
    df_ausgaben_basel = df_ausgaben_basel.iloc[9:-2].reset_index(drop=True)
    df_ausgaben_basel = df_ausgaben_basel.replace("… ", pd.NA).replace("…", pd.NA)
    df_ausgaben_riehen_bettingen = pd.read_excel("data_orig/t13-2-40.xlsx", sheet_name="Ausgaben", usecols="B,H:J")
    df_ausgaben_riehen_bettingen.columns = [
        "jahr",
        "schulexterne_module",
        "schuleigene_module",
        "tagesferien",
    ]
    df_ausgaben_riehen_bettingen["gemeinden"] = "Riehen und Bettingen"
    df_ausgaben_riehen_bettingen = df_ausgaben_riehen_bettingen.iloc[9:-2].reset_index(drop=True)
    df_ausgaben_riehen_bettingen = df_ausgaben_riehen_bettingen.replace("… ", pd.NA).replace("…", pd.NA)
    # Concatenate the two DataFrames
    df_ausgaben = pd.concat([df_ausgaben_basel, df_ausgaben_riehen_bettingen], ignore_index=True)
    df_ausgaben = df_ausgaben.dropna(subset=["schulexterne_module", "schuleigene_module", "tagesferien", "ferienbetreuung"], how='all')
    df_ausgaben.to_csv("data/100455_oeffentliche_ausgaben.csv", index=False)


def process_tagesferien():
    df_tagesferien_stadt_basel = pd.read_excel("data_orig/Gesamtübersicht Tagesferien.xlsx", usecols="A:B, E, H")
    df_tagesferien_stadt_basel.columns = [
        "jahr",
        "anzahl_angebotene_wochen",
        "anzahl_teilnehmende_sus",
        "durschnittliche_teilnahme",
    ]
    df_tagesferien_stadt_basel["gemeinde"] = "Basel"
    df_tagesferien_stadt_basel = df_tagesferien_stadt_basel.iloc[4:-16].reset_index(drop=True)

    df_tagesferien_riehen_bettingen = pd.read_excel("data_orig/Gesamtübersicht Tagesferien.xlsx", usecols="A, C, F, I")
    df_tagesferien_riehen_bettingen.columns = [
        "jahr",
        "anzahl_angebotene_wochen",
        "anzahl_teilnehmende_sus",
        "durschnittliche_teilnahme",
    ]
    df_tagesferien_riehen_bettingen["gemeinde"] = "Riehen und Bettingen"
    df_tagesferien_riehen_bettingen = df_tagesferien_riehen_bettingen.iloc[4:-16].reset_index(drop=True)

    df_tagesferien_kanton_basel_stadt = pd.read_excel("data_orig/Gesamtübersicht Tagesferien.xlsx", usecols="A, D, G, J, L:M")
    df_tagesferien_kanton_basel_stadt.columns = [
        "jahr",
        "anzahl_angebotene_wochen",
        "anzahl_teilnehmende_sus",
        "durschnittliche_teilnahme",
        "ferienbetreuung_total_buchungen",
        "ferienbetreuung_durchschnittlich_teilnehmende",
    ]
    df_tagesferien_kanton_basel_stadt["gemeinde"] = "Kanton Basel-Stadt"
    df_tagesferien_kanton_basel_stadt = df_tagesferien_kanton_basel_stadt.iloc[4:-16].reset_index(drop=True)
    # Concatenate the three DataFrames
    df_tagesferien = pd.concat(
        [df_tagesferien_stadt_basel, df_tagesferien_riehen_bettingen, df_tagesferien_kanton_basel_stadt],
        ignore_index=True
    )

    with open("data/tagesferien_gemeinde_mapping.json", "r", encoding="utf-8") as f:
        tagesferien_gemeinde_mapping = json.load(f)
        # Convert keys to integers
        tagesferien_gemeinde_mapping = {int(k): v for k, v in tagesferien_gemeinde_mapping.items()}
    
    df_tagesferien["anzahl_ferienwochen"] = df_tagesferien["jahr"].map(tagesferien_gemeinde_mapping)
    df_tagesferien.to_csv("data/100456_tagesferien.csv", index=False)
    

def process_anzahl_kinder():
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
    df_kinder = df_kinder.iloc[9:-2].reset_index(drop=True)
    df_kinder = df_kinder.replace("… ", pd.NA)
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
              .str.replace('\u00a0', ' ', regex=False)   # non-breaking space
              .str.replace(',', '.', regex=False)       # Swiss decimals
              .str.replace(r'[^\d.\-]', '', regex=True) # keep digits, dot, minus
              .apply(pd.to_numeric, errors='coerce')
    )


def clean_percent(series: pd.Series) -> pd.Series:
    """
    Clean a Series containing percentages (e.g. '75%', '  12,5 %').
    Returns values in 0–100 range as float.
    """
    return clean_numeric(series.str.replace('%', '', regex=False))


if __name__ == "__main__":
    main()
 