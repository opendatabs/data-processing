import logging
import os

import pandas as pd


def main():
    logging.info("Reading data from source...")
    df = pd.read_csv(os.path.join("data", "Hunde für OGD.csv"), sep=";")

    logging.info("Reading Hundebestandsliste...")
    df_hbl_raw = pd.read_excel(
        os.path.join("data", "Hundebestandsliste seit 1992.xls"),
        engine="xlrd",
        skiprows=5,
    )

    logging.info("Reading Webtabelle for dogs at Gemeinde level...")
    url = "https://statistik.bs.ch/files/webtabellen/t16-2-03.xlsx"

    df_webtabelle = pd.read_excel(
        url,
        sheet_name="Gemeinde",
        skiprows=7,
        usecols="B, D:F",
    )

    logging.info(
        "Extracting jahr, gemeinde_name, hund_geschlecht, hund_geburtsjahr, hund_alter, hund_rasse, hund_farbe..."
    )
    df_hunde = df[
        [
            "jahr",
            "postleitzahl",
            "gemeinde_name",
            "hund_geschlecht",
            "hund_geburtsjahr",
            "hund_alter",
            "hund_rasse",
            "hund_farbe",
        ]
    ]
    # Data cleaning
    df_hunde = df_hunde.copy()  # Ensure we are working on a copy
    df_hunde["hund_geburtsjahr"] = df_hunde["hund_geburtsjahr"].astype(str)  # Explicitly cast to string
    df_hunde["hund_geschlecht"] = df_hunde["hund_geschlecht"].replace("?", "unbekannt")
    df_hunde.loc[df_hunde["hund_alter"] == 888, "hund_geburtsjahr"] = "unbekannt"

    df_hunde["hund_alter"] = df_hunde["hund_alter"].replace(888, pd.NA)
    # Sort values for indexing
    df_hunde = df_hunde.sort_values(
        by=[
            "jahr",
            "gemeinde_name",
            "hund_geschlecht",
            "hund_geburtsjahr",
            "hund_rasse",
            "hund_farbe",
        ]
    ).reset_index(drop=True)
    path_hunde = os.path.join("data", "100444_hunde.csv")
    df_hunde.to_csv(path_hunde, index_label="id", encoding="utf-8-sig")

    df_webtabelle = df_webtabelle.dropna(how="all")
    df_melted = df_webtabelle.melt(id_vars="Gemeinde", var_name="gemeinde_name", value_name="anzahl_hunde")
    df_melted["Gemeinde"] = pd.to_numeric(df_melted["Gemeinde"], errors="coerce")
    df_melted["anzahl_hunde"] = pd.to_numeric(df_melted["anzahl_hunde"], errors="coerce")
    df_webtabelle_2008plus = df_melted[df_melted["Gemeinde"] >= 2008].rename(columns={"Gemeinde": "jahr"})

    df_hbl = df_hbl_raw.copy()
    df_hbl = df_hbl.dropna(how="all")
    df_hbl = df_hbl[df_hbl["Gemeinde"].isin(["Basel", "Riehen", "Bettingen"])]
    df_hbl_long = df_hbl.melt(id_vars="Gemeinde", var_name="jahr", value_name="anzahl_total")
    df_hbl_long["jahr"] = pd.to_numeric(df_hbl_long["jahr"], errors="coerce")
    df_hbl_long = df_hbl_long[df_hbl_long["jahr"] >= 2008].rename(columns={"Gemeinde": "gemeinde_name"})

    # Difference = dogs with unknown Wohnviertel ("unbekannt")
    df_diff = df_hbl_long.merge(df_webtabelle_2008plus, on=["jahr", "gemeinde_name"], how="inner")
    df_diff["anzahl_hunde"] = df_diff["anzahl_total"] - df_diff["anzahl_hunde"]
    df_unbekannt = df_diff[df_diff["anzahl_hunde"] > 0][["jahr", "gemeinde_name", "anzahl_hunde"]].copy()
    df_unbekannt["wohnviertel_aktuell"] = "unbekannt"

    logging.info("Counting number of dogs per year, Wohnviertel and gemeinde...")
    df_hundebestand = (
        df.groupby(["jahr", "wohnviertel_aktuell", "gemeinde_name"]).size().reset_index(name="anzahl_hunde")
    )
    df_hundebestand = pd.concat([df_hundebestand, df_unbekannt], ignore_index=True)
    path_hundestand = os.path.join("data", "100445_hundebestand.csv")
    df_hundebestand.to_csv(path_hundestand, index=False, encoding="utf-8-sig")
    
    logging.info("Counting number of occurences of a dog name per year")
    # First replace NaN, -, ?, 3, 4 with "unbekannt"
    df["hund_name"] = df["hund_name"].replace(["-", "?", "3", "4"], "unbekannt")
    df["hund_name"] = df["hund_name"].fillna("unbekannt")
    df_hundenamen = df.groupby(["jahr", "hund_name"]).size().reset_index(name="anzahl_hunde")
    path_hundenamen = os.path.join("data", "100446_hundenamen.csv")
    df_hundenamen.to_csv(path_hundenamen, index=False, encoding="utf-8-sig")

    df_hundehalter = df_hbl_raw.copy()
    # Tranpose the dataframe and Take the first row as column names and drop it
    df_hundehalter = df_hundehalter.transpose()
    df_hundehalter.columns = df_hundehalter.iloc[0]
    df_hundehalter = df_hundehalter[1:]
    df_hundehalter = df_hundehalter.rename(columns={"Total Hundehalter (neu erhoben ab 2018)": "anzahl_hundehalter"})[
        ["anzahl_hundehalter"]
    ]
    df_hundehalter = df_hundehalter.dropna(subset=["anzahl_hundehalter"])
    df_hundehalter = df_hundehalter.reset_index()
    df_hundehalter = df_hundehalter.rename(columns={"index": "jahr"})
    path_hundehalter = os.path.join("data", "100447_hundehalter.csv")
    df_hundehalter.to_csv(path_hundehalter, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
