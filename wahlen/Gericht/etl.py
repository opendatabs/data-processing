import logging
import os
from io import StringIO

import common
import pandas as pd

from wahlen import credentials


def main():
    # the file we want to change
    ods_ids = [100131, 100132, 100133]
    # create an explicit pattern (2024)
    df_new = pd.read_csv(credentials.template_file_path, delimiter="\t", encoding="ISO-8859-1", index_col=False)
    df_new = pd.DataFrame(columns=df_new.columns)
    # Dictionary für die Spaltenzuordnung erstellen
    mapping = {
        "Wahlbezeichnung": "wahl_titel",
        "Wahltermin": "datum",
        "Anzahl Sitze": "anz_sitze",
        "Bezeichnung Wahlkreis": "gemeinde",
        "Stimmberechtigte Männer": "stimmber_maen",
        "Stimmberechtigte Frauen": "stimmber_fraue",
        "Wahlzettel": "stimmrechtsausweise",
        "Briefliche Stimmabgaben": "ant_brieflich",
        "Ungültige Wahlzettel": "ungueltige",
        "Leere Wahlzettel": "leere",
        "Vereinzelte Stimmen": "vereinzelte",
        "Kandidaten-Nr": "kandidat_nr",
        "Gewählt": "gewaehlt",
        "Name": "name",
        "Vorname": "vorname",
        "Stimmen": "stimmen",
        "Total gültige Wahlzettel": "stimmber_total",
        "Stimmbeteiligung": "stimmbeteiligung",
        "Absolutes Mehr": "absolutes_mehr",
    }
    for Id in ods_ids:
        # read the old file to change it
        url = f"https://data.bs.ch/api/explore/v2.1/catalog/datasets/{Id}/exports/csv?delimiter=%3B&list_separator=%2C&quote_all=false&with_bom=true"
        response = common.requests_get(url, credentials.proxy)
        csv_data = response.content.decode("utf-8")
        df_old = pd.read_csv(StringIO(csv_data), delimiter=";")
        df_new = df_new.assign(**{new_col: df_old[old_col] for new_col, old_col in mapping.items()})
        df_new["Wahlgang"] = df_old["wahlgang"]
        # reform the entries in columns
        df_new["Gewählt"] = df_new["Gewählt"].replace({"ja": "Gewählt", "nein": "Nicht gewählt"})
        new_file_path = os.path.join(credentials.export_file_path, f"{Id}.txt")
        df_new.to_csv(new_file_path, sep="\t", index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
