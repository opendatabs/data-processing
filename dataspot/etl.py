import pandas as pd
import os
import urllib
import common
from dataspot_auth import DataspotAuth
from io import BytesIO
import logging
from dataspot import credentials

def download_datennutzungskatalog_excel():

    auth = DataspotAuth()
    url = auth.get_base_url() + "/api/metadatenmanagement/schemes/Datennutzungskatalog/download?format=xlsx&language=de"
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
        df.columns = df.iloc[0]  # Setze die erste Zeile als Header
        df = df[1:].reset_index(drop=True)  # Entferne die ursprüngliche Header-Zeile
        if "Erstellt von" in df.columns:
                df = df.drop(columns=["Erstellt von"])  # Entferne die Spalte "Erstellt von"
                df.reset_index(drop=True)
        # Allte Tabellen speichern ( Ausser Datensätze und Betandteil) 
        if sheet_name not in ["Datensätze", "Bestandteile"]: 
            save_path = os.path.join(credentials.data_path, f'{sheet_name}.csv')
            df.to_csv(save_path, sep=";", index=False)
        else: 
            if sheet_name == "Datensätze":
                df_data= df
            else: df_bestand = df 


    # Prüfen, ob "Name" in "Bestandteil von" vorkommt und dann Spalte "Bestandteile" erstellen
    df_data["Bestandteile"] = df_data["Name"].apply(
        lambda name: "https://data.bs.ch/explore/dataset/bestandteile_dataspot/table/?sort=bestandteil_von&refine.bestandteil_von=" + encode_for_url(name)
        if name in df_bestand["Bestandteil von"].values else ""
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

    save_path = os.path.join(credentials.data_path, 'Datensaetze.csv') 
    df_data.to_csv(save_path, sep=";", index=False)  
    save_path = os.path.join(credentials.data_path, 'Bestandteile.csv') 
    df_bestand.to_csv(save_path, sep=";", index=False)  

### TO Do 
# save the files in ftp Server 

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
     
