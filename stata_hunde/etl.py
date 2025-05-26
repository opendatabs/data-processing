import logging
import os

import pandas as pd


def main():
    logging.info("Reading data from source...")
    df = pd.read_csv(
        os.path.join("data", "Hunde für OGD.csv"), encoding="windows-1252", sep=";"
    )

    logging.info(
        "Extracting jahr, gemeinde_name, hund_geschlecht, hund_geburtsjahr, hund_alter, hund_rasse, hund_farbe..."
    )
    df_hunde = df[
        [
            "jahr",
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
    df_hunde["hund_geburtsjahr"] = df_hunde["hund_geburtsjahr"].astype(
        str
    )  # Explicitly cast to string
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
    df_hunde.to_csv(path_hunde, index_label="id")

    logging.info("Reading Webtabelle for dogs at Gemeinde level...")
    df_webtabelle = pd.read_excel(
        os.path.join("data", "t16-2-03_Webtabelle_Hunde_seit_1970.xlsx"),
        sheet_name="Gemeinde",
        skiprows=7,
        usecols="B, D:F",
    )
    df_webtabelle = df_webtabelle.dropna(how="all")
    df_melted = df_webtabelle.melt(
        id_vars="Gemeinde", var_name="gemeinde_name", value_name="anzahl_hunde"
    )
    df_melted["Gemeinde"] = pd.to_numeric(df_melted["Gemeinde"], errors="coerce")
    df_melted["anzahl_hunde"] = pd.to_numeric(
        df_melted["anzahl_hunde"], errors="coerce"
    )
    df_filtered = df_melted[df_melted["Gemeinde"] <= 2007]
    df_filtered = df_filtered.rename(columns={"Gemeinde": "jahr"})

    logging.info("Counting number of dogs per year, Wohnviertel and gemeinde...")
    df_hundebestand = (
        df.groupby(["jahr", "wohnviertel_aktuell", "gemeinde_name"])
        .size()
        .reset_index(name="anzahl_hunde")
    )
    df_hundebestand = pd.concat([df_hundebestand, df_filtered], ignore_index=True)
    path_hundestand = os.path.join("data", "100445_hundebestand.csv")
    df_hundebestand.to_csv(path_hundestand, index=False)

    logging.info("Counting number of occurences of a dog name per year")
    # First replace NaN, -, ?, 3, 4 with "unbekannt"
    df["hund_name"] = df["hund_name"].replace(["-", "?", "3", "4"], "unbekannt")
    df["hund_name"] = df["hund_name"].fillna("unbekannt")
    df_hundenamen = (
        df.groupby(["jahr", "hund_name"]).size().reset_index(name="anzahl_hunde")
    )
    path_hundenamen = os.path.join("data", "100446_hundenamen.csv")
    df_hundenamen.to_csv(path_hundenamen, index=False)

    logging.info("Reading number of dog owners per year, reading every column after AB")
    df_hundehalter = pd.read_excel(
        os.path.join("data", "Hundebestandsliste seit 1992.xls"),
        engine="xlrd",
        skiprows=5,
    )
    # Tranpose the dataframe and Take the first row as column names and drop it
    df_hundehalter = df_hundehalter.transpose()
    df_hundehalter.columns = df_hundehalter.iloc[0]
    df_hundehalter = df_hundehalter[1:]
    df_hundehalter = df_hundehalter.rename(
        columns={"Total Hundehalter (neu erhoben ab 2018)": "anzahl_hundehalter"}
    )[["anzahl_hundehalter"]]
    df_hundehalter = df_hundehalter.dropna(subset=["anzahl_hundehalter"])
    df_hundehalter = df_hundehalter.reset_index()
    df_hundehalter = df_hundehalter.rename(columns={"index": "jahr"})
    path_hundehalter = os.path.join("data", "100447_hundehalter.csv")
    df_hundehalter.to_csv(path_hundehalter, index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
