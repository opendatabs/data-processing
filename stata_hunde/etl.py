import logging
import os

import pandas as pd

DATA_DIR = "data"
WEBTABELLE_URL = "https://statistik.bs.ch/files/webtabellen/t16-2-03.xlsx"

WOV_NAME_MAP = {
    1: "Altstadt Grossbasel",
    2: "Vorstädte",
    3: "Am Ring",
    4: "Breite",
    5: "St. Alban",
    6: "Gundeldingen",
    7: "Bruderholz",
    8: "Bachletten",
    9: "Gotthelf",
    10: "Iselin",
    11: "St. Johann",
    12: "Altstadt Kleinbasel",
    13: "Clara",
    14: "Wettstein",
    15: "Hirzbrunnen",
    16: "Rosental",
    17: "Matthäus",
    18: "Klybeck",
    19: "Kleinhüningen",
    20: "Riehen",
    30: "Bettingen",
}


def data_path(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def load_ogd() -> pd.DataFrame:
    logging.info("Reading data from source...")
    return pd.read_csv(data_path("Hunde für OGD.csv"), sep=";")


def load_hundebestandsliste() -> pd.DataFrame:
    logging.info("Reading Hundebestandsliste...")
    return pd.read_excel(
        data_path("Hundebestandsliste seit 1992.xls"),
        engine="xlrd",
        skiprows=5,
    )


def load_webtabelle_gemeinde() -> pd.DataFrame:
    """Webtabelle dog counts by year and Gemeinde (Basel, Riehen, Bettingen)."""
    logging.info("Reading Webtabelle for dogs at Gemeinde level...")
    df = pd.read_excel(
        WEBTABELLE_URL,
        sheet_name="Gemeinde",
        skiprows=7,
        usecols="B, D:F",
    )
    df = df.dropna(how="all")
    df = df.melt(id_vars="Gemeinde", var_name="gemeinde_name", value_name="anzahl_hunde")
    df["Gemeinde"] = pd.to_numeric(df["Gemeinde"], errors="coerce")
    df["anzahl_hunde"] = pd.to_numeric(df["anzahl_hunde"], errors="coerce")
    return df


def load_unbekannt_wohnviertel() -> pd.DataFrame:
    """Hardcoded counts for dogs with unknown Wohnviertel (2008+)."""
    df = pd.read_excel(data_path("Hunde_Wohniertel_unbekannt.xlsx"))
    df = df.rename(columns={"Jahr": "jahr", "unbekannt": "anzahl_hunde"})
    df["gemeinde_name"] = "unbekannt"
    df["wohnviertel_aktuell"] = "unbekannt"
    return df[["jahr", "gemeinde_name", "wohnviertel_aktuell", "anzahl_hunde"]]


def add_wov_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["wov_name"] = (
        pd.to_numeric(df["wohnviertel_aktuell"], errors="coerce").map(WOV_NAME_MAP)
    )
    df.loc[df["wohnviertel_aktuell"] == "unbekannt", "wov_name"] = "unbekannt"
    return df


def write_hunde(df: pd.DataFrame) -> None:
    logging.info(
        "Extracting jahr, gemeinde_name, hund_geschlecht, "
        "hund_geburtsjahr, hund_alter, hund_rasse, hund_farbe..."
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
    ].copy()

    df_hunde["hund_geburtsjahr"] = df_hunde["hund_geburtsjahr"].astype(str)
    df_hunde["hund_geschlecht"] = df_hunde["hund_geschlecht"].replace("?", "unbekannt")
    df_hunde.loc[df_hunde["hund_alter"] == 888, "hund_geburtsjahr"] = "unbekannt"
    df_hunde["hund_alter"] = df_hunde["hund_alter"].replace(888, pd.NA)

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

    df_hunde.to_csv(data_path("100444_hunde.csv"), index_label="id", encoding="utf-8-sig")


def write_hundebestand(
    df: pd.DataFrame,
    df_webtabelle: pd.DataFrame,
    df_unbekannt: pd.DataFrame,
) -> None:
    logging.info("Counting number of dogs per year, Wohnviertel and Gemeinde...")

    # 2008+: individual records grouped by Wohnviertel and Gemeinde
    df_2008plus = (
        df.groupby(["jahr", "wohnviertel_aktuell", "gemeinde_name"])
        .size()
        .reset_index(name="anzahl_hunde")
    )

    # ≤2007: only Gemeinde-level totals are available (no Wohnviertel breakdown)
    df_pre2008 = (
        df_webtabelle[df_webtabelle["Gemeinde"] <= 2007]
        .rename(columns={"Gemeinde": "jahr"})
    )

    df_hundebestand = pd.concat(
        [df_2008plus, df_unbekannt, df_pre2008],
        ignore_index=True,
    )
    df_hundebestand["jahr"] = pd.to_numeric(df_hundebestand["jahr"], errors="coerce").astype(int)
    df_hundebestand = add_wov_names(df_hundebestand)

    df_hundebestand.to_csv(data_path("100445_hundebestand.csv"), index=False, encoding="utf-8-sig")


def write_hundenamen(df: pd.DataFrame) -> None:
    logging.info("Counting number of occurences of a dog name per year")
    df = df.copy()
    df["hund_name"] = df["hund_name"].replace(["-", "?", "3", "4"], "unbekannt")
    df["hund_name"] = df["hund_name"].fillna("unbekannt")

    df_hundenamen = df.groupby(["jahr", "hund_name"]).size().reset_index(name="anzahl_hunde")
    df_hundenamen.to_csv(data_path("100446_hundenamen.csv"), index=False, encoding="utf-8-sig")


def write_hundehalter(df_hbl_raw: pd.DataFrame) -> None:
    df_hundehalter = df_hbl_raw.transpose()
    df_hundehalter.columns = df_hundehalter.iloc[0]
    df_hundehalter = df_hundehalter[1:]
    df_hundehalter = df_hundehalter.rename(
        columns={"Total Hundehalter (neu erhoben ab 2018)": "anzahl_hundehalter"}
    )[["anzahl_hundehalter"]]
    df_hundehalter = df_hundehalter.dropna(subset=["anzahl_hundehalter"]).reset_index()
    df_hundehalter = df_hundehalter.rename(columns={"index": "jahr"})

    df_hundehalter.to_csv(data_path("100447_hundehalter.csv"), index=False, encoding="utf-8-sig")


def main() -> None:
    df = load_ogd()
    df_hbl_raw = load_hundebestandsliste()
    df_webtabelle = load_webtabelle_gemeinde()
    df_unbekannt = load_unbekannt_wohnviertel()

    write_hunde(df)
    write_hundebestand(df, df_webtabelle, df_unbekannt)
    write_hundenamen(df)
    write_hundehalter(df_hbl_raw)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
