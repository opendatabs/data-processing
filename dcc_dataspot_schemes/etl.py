import logging
import os
import urllib
from io import BytesIO

import common
import pandas as pd
from dataspot_auth import DataspotAuth

ods_id = {
    "Datensaetze": 100433,
    "Bestandteile": 100434,
    "Sammlungen": 100435,
    "Distributionen": 100436,
    "Verantwortungen": 100437,
    "Zusatzinformationen": 100438,
    "Bereitstellungen": 100439,
}
S_id = pd.Series(data=ods_id)


def download_datennutzungskatalog_excel():
    auth = DataspotAuth()
    url = "https://datenkatalog.bs.ch/api/prod/schemes/Datenprodukte/download?format=xlsx&language=de&status=PUBLISHED"
    headers = auth.get_headers()
    response = common.requests_get(url=url, headers=headers)
    return response.content


# Funktion zur URL-Kodierung
def encode_for_url(text):
    if pd.isna(text):  # Falls der Wert NaN ist
        return ""
    return urllib.parse.quote(text.strip())


def main():
    data = download_datennutzungskatalog_excel()
    excel_data = pd.read_excel(BytesIO(data), sheet_name=None)
    # Dictionary mit Sheet-Namen als Schlüssel und DataFrames als Werte
    dataframes = {sheet: pd.DataFrame(data) for sheet, data in excel_data.items()}
    # Ausgabe der Namen der Sheets
    print("Folgende Sheets wurden geladen:", list(dataframes.keys()))
    # Ausgabe der ersten Zeilen jedes Sheets
    for sheet_name, df in dataframes.items():
        if sheet_name == "__literals__":
            continue  # ignorieren
        df.columns = df.iloc[0]  # Setze die erste Zeile als Header
        df = df[1:].reset_index(drop=True)  # Entferne die ursprüngliche Header-Zeile
        if "Erstellt von" in df.columns:
            df = df.drop(columns=["Erstellt von"])  # Entferne die Spalte "Erstellt von"
            df.reset_index(drop=True)
        # Allte Tabellen speichern (Ausser Datensätze und Betandteil)
        if sheet_name not in ["Datensätze", "Bestandteile"]:
            save_path = os.path.join("data", f"{S_id[sheet_name]}_{sheet_name}.csv")
            df.to_csv(save_path, sep=";", index=False)
            # In FTP Server speichern
            common.update_ftp_and_odsp(save_path, "dataspot", S_id[sheet_name])
        else:
            if sheet_name == "Datensätze":
                df_data = df
            else:
                df_bestand = df

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

    save_path = os.path.join("data", f"{S_id['Datensaetze']}_Datensaetze.csv")
    df_data.to_csv(save_path, sep=";", index=False)
    common.update_ftp_and_odsp(save_path, "dataspot", S_id["Datensaetze"], unpublish_first=True)
    save_path = os.path.join("data", f"{S_id['Bestandteile']}_Bestandteile.csv")
    df_bestand.to_csv(save_path, sep=";", index=False)
    common.update_ftp_and_odsp(save_path, "dataspot", S_id["Bestandteile"], unpublish_first=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
