import logging
import os

import pandas as pd

DATA_DIR = "data"
SOURCE_FILE = "Hunde_alle.csv"
WEBTABELLE_URL = "https://statistik.bs.ch/files/webtabellen/t16-2-03.xlsx"

VALID_GEMEINDE = {"Basel", "Riehen", "Bettingen"}
TEXT_WOV_PREFIX = "Wohnviertel in "

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


def normalize_wohnviertel(value) -> int | None:
    try:
        wov = int(float(value))
    except (ValueError, TypeError):
        return None
    if wov in range(1, 20) or wov in (20, 30):
        return wov
    return None


def is_groupable_dog(row: pd.Series) -> bool:
    wov_str = str(row["wohnviertel_aktuell"])
    if wov_str.startswith(TEXT_WOV_PREFIX):
        return False
    if row["gemeinde_name"] not in VALID_GEMEINDE:
        return False
    return normalize_wohnviertel(row["wohnviertel_aktuell"]) is not None


def load_source() -> pd.DataFrame:
    logging.info("Reading data from source...")
    return pd.read_csv(data_path(SOURCE_FILE), sep=";", low_memory=False)


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


def load_webtabelle_kanton() -> pd.DataFrame:
    """Webtabelle dog counts by year for Kanton Basel-Stadt."""
    df = pd.read_excel(
        WEBTABELLE_URL,
        sheet_name="Gemeinde",
        skiprows=7,
        usecols="B, G",
    )
    df = df.dropna(how="all")
    df.columns = ["jahr", "anzahl_hunde"]
    df["jahr"] = pd.to_numeric(df["jahr"], errors="coerce")
    df["anzahl_hunde"] = pd.to_numeric(df["anzahl_hunde"], errors="coerce")
    return df.dropna(subset=["jahr", "anzahl_hunde"])


def load_webtabelle_wohnviertel() -> pd.DataFrame:
    """Webtabelle dog counts by year and Wohnviertel (2008+)."""
    df = pd.read_excel(
        WEBTABELLE_URL,
        sheet_name="Wohnviertel",
        skiprows=7,
    )
    df = df.dropna(how="all")
    df = df.rename(columns={"Gemeinde, Wohnviertel": "wov_name"})
    df = df[df["wov_name"].isin(set(WOV_NAME_MAP.values()))]

    year_cols = [col for col in df.columns if isinstance(col, int)]
    df = df[["wov_name", *year_cols]]
    df = df.melt(id_vars="wov_name", var_name="jahr", value_name="anzahl_hunde")
    df["jahr"] = pd.to_numeric(df["jahr"], errors="coerce")
    df["anzahl_hunde"] = pd.to_numeric(df["anzahl_hunde"], errors="coerce")

    name_to_wov = {name: wov for wov, name in WOV_NAME_MAP.items()}
    df["wohnviertel_aktuell"] = df["wov_name"].map(name_to_wov)
    df["gemeinde_name"] = df["wohnviertel_aktuell"].apply(
        lambda wov: "Basel" if wov in range(1, 20) else ("Riehen" if wov == 20 else "Bettingen")
    )
    return df.dropna(subset=["jahr", "anzahl_hunde", "wohnviertel_aktuell"])[
        ["jahr", "wohnviertel_aktuell", "gemeinde_name", "anzahl_hunde"]
    ]


def webtabelle_totals_by_year(df_webtabelle: pd.DataFrame) -> pd.Series:
    return df_webtabelle.groupby("Gemeinde")["anzahl_hunde"].sum().rename("anzahl_webtabelle")


def calculate_unbekannt(df: pd.DataFrame, df_webtabelle: pd.DataFrame) -> pd.DataFrame:
    """Dogs with unknown Wohnviertel: source total minus Webtabelle total per year."""
    totals = df.groupby("jahr").size().rename("anzahl_source")
    web = webtabelle_totals_by_year(df_webtabelle)

    df_unbekannt = pd.concat([totals, web], axis=1).dropna(subset=["anzahl_webtabelle"]).reset_index(names="jahr")
    df_unbekannt["jahr"] = df_unbekannt["jahr"].astype(int)
    df_unbekannt["anzahl_hunde"] = df_unbekannt["anzahl_source"] - df_unbekannt["anzahl_webtabelle"]
    df_unbekannt = df_unbekannt[df_unbekannt["anzahl_hunde"] > 0]

    df_unbekannt["gemeinde_name"] = "unbekannt"
    df_unbekannt["wohnviertel_aktuell"] = "unbekannt"
    return df_unbekannt[["jahr", "gemeinde_name", "wohnviertel_aktuell", "anzahl_hunde"]]


def add_wov_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["wov_name"] = pd.to_numeric(df["wohnviertel_aktuell"], errors="coerce").map(WOV_NAME_MAP)
    df.loc[df["wohnviertel_aktuell"] == "unbekannt", "wov_name"] = "unbekannt"
    return df


def write_hunde(df: pd.DataFrame) -> None:
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
    ].copy()

    # Keep only official Gemeinden in this field; everything else becomes empty.
    df_hunde.loc[~df_hunde["gemeinde_name"].isin(VALID_GEMEINDE), "gemeinde_name"] = ""

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


def build_kanton_rows(
    df_kanton: pd.DataFrame,
    df_unbekannt: pd.DataFrame,
) -> pd.DataFrame:
    """Kanton total per year: Webtabelle value (≤2007), plus unbekannt from 2008."""
    df_pre2008 = df_kanton[df_kanton["jahr"] <= 2007].copy()

    df_2008plus = df_kanton[df_kanton["jahr"] >= 2008].merge(
        df_unbekannt[["jahr", "anzahl_hunde"]].rename(columns={"anzahl_hunde": "anzahl_unbekannt"}),
        on="jahr",
        how="inner",
    )
    df_2008plus["anzahl_hunde"] = df_2008plus["anzahl_hunde"] + df_2008plus["anzahl_unbekannt"]
    df_2008plus = df_2008plus[["jahr", "anzahl_hunde"]]

    df_kanton_rows = pd.concat([df_pre2008, df_2008plus], ignore_index=True)
    df_kanton_rows["gemeinde_name"] = "Kanton Basel-Stadt"
    return df_kanton_rows[["jahr", "gemeinde_name", "anzahl_hunde"]]


def write_hundebestand(
    df: pd.DataFrame,
    df_webtabelle: pd.DataFrame,
    df_kanton: pd.DataFrame,
    df_webtabelle_wohnviertel: pd.DataFrame,
) -> None:
    logging.info("Counting number of dogs per year, Wohnviertel and Gemeinde...")

    # 2008+: use Wohnviertel counts directly from Webtabelle.
    df_2008plus = df_webtabelle_wohnviertel.copy()
    df_unbekannt = calculate_unbekannt(
        df[df["jahr"] >= 2008],
        df_webtabelle[df_webtabelle["Gemeinde"] >= 2008],
    )
    df_kanton_rows = build_kanton_rows(df_kanton, df_unbekannt)

    # ≤2007: only Gemeinde-level totals are available (no Wohnviertel breakdown)
    df_pre2008 = df_webtabelle[df_webtabelle["Gemeinde"] <= 2007].rename(columns={"Gemeinde": "jahr"})

    df_hundebestand = pd.concat(
        [df_2008plus, df_unbekannt, df_pre2008, df_kanton_rows],
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
    df_hundehalter = df_hundehalter.rename(columns={"Total Hundehalter (neu erhoben ab 2018)": "anzahl_hundehalter"})[
        ["anzahl_hundehalter"]
    ]
    df_hundehalter = df_hundehalter.dropna(subset=["anzahl_hundehalter"]).reset_index()
    df_hundehalter = df_hundehalter.rename(columns={"index": "jahr"})

    df_hundehalter.to_csv(data_path("100447_hundehalter.csv"), index=False, encoding="utf-8-sig")


def main() -> None:
    df = load_source()
    df_hbl_raw = load_hundebestandsliste()
    df_webtabelle = load_webtabelle_gemeinde()
    df_kanton = load_webtabelle_kanton()
    df_webtabelle_wohnviertel = load_webtabelle_wohnviertel()

    write_hunde(df)
    write_hundebestand(df, df_webtabelle, df_kanton, df_webtabelle_wohnviertel)
    write_hundenamen(df)
    write_hundehalter(df_hbl_raw)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
