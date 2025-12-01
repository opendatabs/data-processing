import logging
import os
import urllib
from io import BytesIO

import common
import pandas as pd
from dataspot_auth import DataspotAuth

models = ['Datenprodukte', 'Fachdaten', 'Referenzdaten', 'Systeme', 'Datenbankobjekte','Kennzahlen', 'Datentypen (technisch)','Datentypen (fachlich)' ]

ods_id = {
    "Fachdaten_Attribute": 100490,
    "Fachdaten_Beziehungen": 100491,
    "Fachdaten_Geschaeftsobjekte": 100492,
    "Fachdaten_Verantwortungen": 100493,
    "Datenbankobjekte_Attribute": 100494,
    "Datenbankobjekte_Bereitstellungen": 100495,
    "Datenbankobjekte_Datenobjekte": 100496,
    "Datenbankobjekte_Zusatzinformationen": 100497,
    "Datentypen_fachlich_Datentypen": 100498,
    "Datentypen_fachlich_Zusatzinformationen": 100499,
    "Datentypen_technisch_Datentypen": 100500,
    "Kennzahlen_Verantwortungen": 100501,
    "Referenzdaten_Elemente": 100502,
    "Referenzdaten_Referenzwerte": 100503,
    "Referenzdaten_Verantwortungen": 100504,
    "Systeme_Abhaengigkeiten": 100505,
    "Systeme_Systeme": 100506,
    "Systeme_Verantwortungen": 100507,
    "Datenprodukte_Datensaetze": 100433,
    "Datenprodukte_Bestandteile": 100434,
    "Datenprodukte_Sammlungen": 100435,
    "Datenprodukte_Distributionen": 100436,
    "Datenprodukte_Verantwortungen": 100437,
    "Datenprodukte_Bereitstellungen": 100439,
    "Datenprodukte_Zusatzinformationen": 100438,
  
}

S_id = pd.Series(data=ods_id)

Sammlungen_seen = False


# Funktion zur Normalisierung von Sheet-Namen
def normalize_name(name: str) -> str:
    mapping = {"ä": "ae", "Ä": "Ae", "ö": "oe", "Ö": "Oe", "ü": "ue", "Ü": "Ue", "ß": "ss"}
    for k, v in mapping.items():
        name = name.replace(k, v)
    name = (
        name.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace("\\", "_").replace(":", "_")
    )
    return name


# Funktion zum Herunterladen der Excel-Datei aus dem Datenkatalog
def download_datennutzungskatalog_excel(model):
    auth = DataspotAuth()
    url = f"https://datenkatalog.bs.ch/api/prod/schemes/{model}/download?format=xlsx&language=de&status=PUBLISHED"
    headers = auth.get_headers()
    response = common.requests_get(url=url, headers=headers)
    return response.content


# Funktion zur URL-Kodierung
def encode_for_url(text):
    if pd.isna(text):  # Falls der Wert NaN ist
        return ""
    return urllib.parse.quote(text.strip())


# Funktion zur Vorbereitung und Zusammenführung der Datensätze
def prepare_and_merge_datasets(df_data, df_bestand):
    # Prüfen, ob "Name" in "Bestandteil von" vorkommt und dann Spalte "Bestandteile" erstellen
    df_data["Bestandteile"] = df_data["Name"].apply(
        lambda name: "https://data.bs.ch/explore/dataset/100434/table/?refine.bestandteil_von=" + encode_for_url(name)
        if name in df_bestand["Bestandteil von"].values
        else ""
    )
    # Ersetze Zeilenumbrüche in der Spalte "Schlüsselwörter" mit Kommas
    df_data["Schlüsselwörter"] = df_data["Schlüsselwörter"].astype(str).str.replace(r"[\n\r]+", ", ", regex=True)
    # NaN-Werte in leere Strings umwandeln
    df_data["Schlüsselwörter"] = df_data["Schlüsselwörter"].replace("nan", "").replace("NaN", "")
    # Hole die Spaltenliste
    columns = list(df_data.columns)
    # Bestimme die letzte Spalte
    last_column = columns[-1]
    # Neue Reihenfolge: Erste Spalte bleibt gleich, dann die letzte Spalte, dann der Rest
    new_order = [columns[0], last_column] + columns[1:-1]
    # Wende die neue Spaltenreihenfolge auf das DataFrame an
    df_data = df_data[new_order]
    return df_data, df_bestand


# Funktion zum Speichern der Datenprodukte als CSV-Dateien
def save_datenprodukte(dataframes, model):
    global Sammlungen_seen
    # Ausgabe der Namen der Sheets
    logging.info("Folgende Sheets wurden geladen: %s", list(dataframes.keys()))
    for sheet, df in dataframes.items():
        sheet = normalize_name(sheet)
        model = normalize_name(model)
        if sheet == "__literals__":
            continue  # ignorieren
        if sheet == "Sammlungen":
            if Sammlungen_seen:
                continue  # ignorieren
            Sammlungen_seen = True
        df.columns = df.iloc[0]  # Setze die erste Zeile als Header
        df = df[1:].reset_index(drop=True)  # Entferne die ursprüngliche Header-Zeile
        # Entferne die Spalte "Verantwortlich", falls vorhanden
        if "Verantwortlich" in df.columns:
            df = df.drop(columns=["Verantwortlich"])
        if "Erstellt von" in df.columns:
            df = df.drop(columns=["Erstellt von"])  # Entferne die Spalte "Erstellt von"
            df.reset_index(drop=True)
            # Allte Tabellen speichern (Ausser Datensätze und Betandteil)
        if sheet not in ["Datensaetze", "Bestandteile"]:
            save_path = os.path.join("data", f"{S_id[f'{model}_{sheet}']}_{model}_{sheet}.csv")
            df.to_csv(save_path, sep=";", index=False)
            # In FTP Server speichern
            common.update_ftp_and_odsp(save_path, "dataspot", S_id[f"{model}_{sheet}"])
        else:
            if sheet == "Datensaetze":
                df_data = df
            else:
                df_bestand = df

    # Speichern der bearbeiteten Datensätze und Bestandteile
    if "df_data" in locals() and "df_bestand" in locals():
        df_data, df_bestand = prepare_and_merge_datasets(df_data, df_bestand)
        save_path = os.path.join("data", f"{S_id[f'{model}_Datensaetze']}_{model}_Datensaetze.csv")
        df_data.to_csv(save_path, sep=";", index=False)
        # In FTP Server speichern
        common.update_ftp_and_odsp(save_path, "dataspot", S_id[f"{model}_Datensaetze"])
        save_path = os.path.join("data", f"{S_id[f'{model}_Bestandteile']}_{model}_Bestandteile.csv")
        df_bestand.to_csv(save_path, sep=";", index=False)
        # In FTP Server speichern
        common.update_ftp_and_odsp(save_path, "dataspot", S_id[f"{model}_Bestandteile"])


def main():
    for model in models:
        data = download_datennutzungskatalog_excel(model)
        excel_data = pd.read_excel(BytesIO(data), sheet_name=None)
        # Dictionary mit Sheet-Namen als Schlüssel und DataFrames als Werte
        dataframes = {sheet: pd.DataFrame(data) for sheet, data in excel_data.items()}
        save_datenprodukte(dataframes, model)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
