import ast
import logging
import os
import time
from datetime import datetime

import common
import numpy as np
import pandas as pd

# All paths
BASE_PATH = "https://grosserrat.bs.ch"
PATH_PERSONEN = f"{BASE_PATH}/?mnr="
PATH_GESCHAEFT = f"{BASE_PATH}/?gnr="
PATH_DOKUMENT = f"{BASE_PATH}/?dnr="
PATH_MEDIA = f"{BASE_PATH}/media/files"
PATH_MEDIA_TAGESORDNUNG = f"{PATH_MEDIA}/tagesordnungen/"
PATH_MEDIA_RATSPROTOKOLLE = f"{PATH_MEDIA}/ratsprotokolle/"
PATH_MEDIA_AUDIO = "http://protokolle.grosserrat-basel.ch//"

PATH_DATASET = "https://data.bs.ch/explore/dataset/"

# Unix timestamps that mark the maximum and minimum possible timestamp
UNIX_TS_MAX = "253402300799"
UNIX_TS_MIN = "-30610224000"

# Dictionary to handle the comittees which need their ID to be replaced
REPLACE_UNI_NR_GRE_DICT = {
    "1934": "3",
    "4276": "2910",
    "4278": "3164",
    "4279": "3196",
    "4280": "3331",
    "4252": np.nan,
    "4274": np.nan,
    "4283": np.nan,
}
MEMBERS_MISSING = [
    {
        "uni_nr": "4016",
        "vorname": "Annemarie",
        "name": "Burckhardt",
        "name_vorname": "Burckhardt, Annemarie",
        "anrede": "Frau",
    },
    {
        "uni_nr": "4018",
        "vorname": "Hans Rudolf",
        "name": "Bachmann",
        "name_vorname": "Bachmann, Hans Rudolf",
        "anrede": "Herr",
    },
    {
        "uni_nr": "4019",
        "vorname": "Christoph",
        "name": "Eymann",
        "name_vorname": "Eymann, Christoph",
        "anrede": "Herr",
    },
    {
        "uni_nr": "4021",
        "vorname": "Markus",
        "name": "Ritter",
        "name_vorname": "Ritter, Markus",
        "anrede": "Herr",
    },
    {
        "uni_nr": "4024",
        "vorname": "Umberto",
        "name": "Stücklin",
        "name_vorname": "Stücklin, Umberto",
        "anrede": "Herr",
    },
    {
        "uni_nr": "4025",
        "vorname": "Martin H.",
        "name": "Burckhardt",
        "name_vorname": "Burckhardt, Martin H.",
        "anrede": "Herr",
    },
    {
        "uni_nr": "4031",
        "vorname": "Christoph",
        "name": "Stutz",
        "name_vorname": "Stutz, Christoph",
        "anrede": "Herr",
    },
    {
        "uni_nr": "4044",
        "vorname": "Eleonore",
        "name": "Schaub",
        "name_vorname": "Schaub, Eleonore",
        "anrede": "Frau",
    },
    {
        "uni_nr": "4045",
        "vorname": "Alice",
        "name": "Schaub",
        "name_vorname": "Schaub, Alice",
        "anrede": "Frau",
    },
]
DF_MEMBERS_MISSING = pd.DataFrame(MEMBERS_MISSING)

REPLACE_STATUS_CODES_GES = {"A": "Abgeschlossen", "B": "In Bearbeitung"}
REPLACE_STATUS_CODES_ZUW = {
    "A": "Abgeschlossen",
    "B": "In Bearbeitung",
    "X": "Abgebrochen",
    "F": "Fertig",
}
REPLACE_STATUS_CODES_TRAKT = {
    "0": "offen",
    "1": "erledigt",
    "2": "verschoben",
    "3": "2. Lesung",
    "4": "neu",
    "5": "abgesetzt",
    "6": "zurückgezogen",
    "7": "in Behandlung",
}


def main():
    """
    Reads various CSV files with diff. types of data, processes them, and creates
    corresponding CSV files to publish them.

    1. Read CSV files containing data about addresses, memberships, committees, interests, businesses,
       associates, assignments, documents, processes, and meetings.
    2. Process and modify the data
    3. Create CSV files for data.bs.ch
    """
    logging.info("Reading Personen.csv...")
    df_adr = common.pandas_read_csv(get_path_to_file("adr"), encoding="utf-8", dtype=str)
    logging.info("Reading Mitgliedschaften.csv...")
    df_mit = common.pandas_read_csv(get_path_to_file("mit"), encoding="utf-8", dtype=str)
    logging.info("Reading Gremien.csv...")
    df_gre = common.pandas_read_csv(get_path_to_file("gre"), encoding="utf-8", dtype=str)
    logging.info("Reading Interessensbindungen.csv...")
    df_intr = common.pandas_read_csv(get_path_to_file("intr"), encoding="utf-8", dtype=str)

    logging.info("Reading Geschäfte.csv...")
    df_ges = common.pandas_read_csv(get_path_to_file("ges"), encoding="utf-8", dtype=str)
    # Replace identifiers to match with values in the committee list (gremium.csv)
    df_ges["gr_urheber"] = df_ges["gr_urheber"].replace(REPLACE_UNI_NR_GRE_DICT)

    logging.info("Reading Konsorten.csv...")
    df_kon = common.pandas_read_csv(get_path_to_file("kon"), encoding="utf-8", dtype=str)
    df_kon["uni_nr_adr"] = df_kon["uni_nr_adr"].replace(REPLACE_UNI_NR_GRE_DICT)

    logging.info("Reading Zuweisungen.csv...")
    df_zuw = common.pandas_read_csv(get_path_to_file("zuw"), encoding="utf-8", dtype=str)
    # Replace identifiers to match with values in the committee list (gremium.csv)
    df_zuw["uni_nr_an"] = df_zuw["uni_nr_an"].replace(REPLACE_UNI_NR_GRE_DICT)
    df_zuw["uni_nr_von"] = df_zuw["uni_nr_von"].replace(REPLACE_UNI_NR_GRE_DICT)

    logging.info("Reading Dokumente.csv...")
    df_dok = common.pandas_read_csv(get_path_to_file("dok"), encoding="utf-8", dtype=str)
    logging.info("Reading Vorgänge.csv...")
    df_vor = common.pandas_read_csv(get_path_to_file("vor"), encoding="utf-8", dtype=str)
    logging.info("Reading Sitzungen.csv...")
    df_siz = common.pandas_read_csv(get_path_to_file("siz"), encoding="utf-8", dtype=str)
    logging.info("Reading Sitzungsdaten.csv...")
    df_gr_sitzung = common.pandas_read_csv(get_path_to_file("gr_sitzung"), encoding="utf-8", dtype=str)
    logging.info("Reading Tagesordnung.csv...")
    df_gr_tagesordnung = common.pandas_read_csv(get_path_to_file("gr_tagesordnung"), encoding="utf-8", dtype=str)
    logging.info("Reading Traktanden.csv...")
    df_gr_traktanden = common.pandas_read_csv(get_path_to_file("gr_tagesordnung_pos"), encoding="utf-8", dtype=str)

    # Perform data processing and CSV file creation functions
    args_for_uploads = [
        create_mitglieder_csv(df_adr, df_mit),
        create_mitgliedschaften_csv(df_adr, df_mit, df_gre),
        create_interessensbindungen_csv(df_adr, df_intr),
        create_gremien_csv(df_gre, df_mit),
        create_geschaefte_csv(df_adr, df_ges, df_kon, df_gre),
        create_zuweisungen_csv(df_gre, df_ges, df_zuw),
        create_dokumente_csv(df_ges, df_dok),
        create_vorgaenge_csv(df_ges, df_vor, df_siz),
        create_traktanden_csv(df_gr_tagesordnung, df_gr_traktanden, df_gr_sitzung),
    ]

    # Upload everything into FTP-Server and update the dataset on data.bs.ch
    for args_for_upload in args_for_uploads:
        common.update_ftp_and_odsp(*args_for_upload)


def get_path_to_file(chosentable: str) -> str:
    path_gr = f"{BASE_PATH}/index.php?option=com_gribs&view=exporter&format=csv&chosentable="
    return f"{path_gr}{chosentable}"


def create_mitglieder_csv(df_adr: pd.DataFrame, df_mit: pd.DataFrame) -> tuple:
    # Select members of Grosser Rat without specific functions
    # since functions are always recorded as part of an entire membership
    # Not ignoring it would lead to duplicated memberships
    df_gr = df_mit[(df_mit["uni_nr_gre"] == "3") & (df_mit["funktion"].isna())]
    df = pd.merge(df_adr, df_gr, left_on="uni_nr", right_on="uni_nr_adr")

    # Rename columns for clarity
    df = df.rename(columns={"beginn": "gr_beginn", "ende": "gr_ende"})

    # Get current date's Unix timestamp
    current_unix_timestamp = int(time.time())
    # Check if the membership is currently active in Grosser Rat
    df["ist_aktuell_grossrat"] = df["gr_ende"].apply(lambda end: "Ja" if int(end) > current_unix_timestamp else "Nein")

    # Create url's
    df["url"] = PATH_PERSONEN + df["uni_nr"]
    df["url_gremiumsmitgliedschaften"] = PATH_DATASET + "100308/?refine.uni_nr_adr=" + df["uni_nr"]
    df["url_interessensbindungen"] = PATH_DATASET + "100309/?refine.uni_nr=" + df["uni_nr"]
    df["url_urheber"] = PATH_DATASET + "100311/?refine.nr_urheber=" + df["uni_nr"]

    # append "name" and "vorname"
    df["name_vorname"] = df["name"] + ", " + df["vorname"]

    # Make sure there are no duplicates in the "Titel"
    df["titel"] = df["titel"].str.replace(". ", ".", regex=False).str.replace(".", ". ", regex=False)
    df["titel"] = df["titel"].str.replace(" ,", ",", regex=False)
    df["titel"] = df["titel"].str.replace("pol.", "pol", regex=False).str.replace("pol", "pol.", regex=False)
    df["titel"] = df["titel"].str.rstrip()

    # Select relevant columns for publication
    cols_of_interest = [
        "ist_aktuell_grossrat",
        "anrede",
        "titel",
        "name",
        "vorname",
        "name_vorname",
        "gebdatum",
        "gr_sitzplatz",
        "gr_wahlkreis",
        "partei",
        "partei_kname",
        "gr_beginn",
        "gr_ende",
        "url",
        "uni_nr",
        "strasse",
        "plz",
        "ort",
        "gr_beruf",
        "gr_arbeitgeber",
        "homepage",
        "url_gremiumsmitgliedschaften",
        "url_interessensbindungen",
        "url_urheber",
    ]
    df = df[cols_of_interest]

    # Convert dates in string or unix timestamp to Datetime
    df["gebdatum"] = pd.to_datetime(df["gebdatum"], format="%d.%m.%Y")
    df = unix_to_datetime(df, ["gr_beginn", "gr_ende"])

    logging.info('Creating dataset "Grosser Rat: Ratsmitgliedschaften"...')
    path_export = os.path.join("data", "export", "100307_gr_mitglieder.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100307"


def create_mitgliedschaften_csv(df_adr: pd.DataFrame, df_mit: pd.DataFrame, df_gre: pd.DataFrame) -> tuple:
    df = pd.merge(df_gre, df_mit, left_on="uni_nr", right_on="uni_nr_gre")
    df = pd.merge(df, df_adr, left_on="uni_nr_adr", right_on="uni_nr")

    # Drop every member of a Gremium which was never in the Grossrat (Parlamentsdienst)
    df = df.groupby("uni_nr_adr").filter(lambda x: (x["uni_nr_gre"] == "3").any())

    # Rename columns for clarity
    df = df.rename(
        columns={
            "name_x": "name_gre",
            "name_y": "name_adr",
            "beginn": "beginn_mit",
            "ende": "ende_mit",
            "kurzname": "kurzname_gre",
            "vorname": "vorname_adr",
            "funktion": "funktion_adr",
        }
    )

    # Create url's
    df["url_adr"] = PATH_PERSONEN + df["uni_nr_adr"]
    # URL for committee page (currently removed)
    # df['url_gre'] = credentials.path_gremien + df['uni_nr_gre']
    df["url_gremium"] = PATH_DATASET + "100310/?refine.uni_nr=" + df["uni_nr_gre"]
    df["url_ratsmitgliedschaften"] = PATH_DATASET + "100307/?refine.uni_nr=" + df["uni_nr_adr"]

    # append "name" and "vorname"
    df["name_vorname"] = df["name_adr"] + ", " + df["vorname_adr"]

    # Select relevant columns for publication
    cols_of_interest = [
        "kurzname_gre",
        "name_gre",
        "gremientyp",
        "uni_nr_gre",
        "url_gremium",
        "beginn_mit",
        "ende_mit",
        "funktion_adr",
        "anrede",
        "name_adr",
        "vorname_adr",
        "name_vorname",
        "partei_kname",
        "url_adr",
        "uni_nr_adr",
        "url_ratsmitgliedschaften",
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ["beginn_mit", "ende_mit"])

    logging.info('Creating dataset "Grosser Rat: Mitgliedschaften in Gremien"...')
    path_export = os.path.join("data", "export", "100308_gr_mitgliedschaften.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100308"


def create_interessensbindungen_csv(df_adr: pd.DataFrame, df_intr: pd.DataFrame) -> tuple:
    df = pd.merge(df_intr, df_adr, left_on="idnr_adr", right_on="idnr")

    # Splitting 'text' column to separate 'intr-bind' and 'funktion'
    df[["intr-bind", "funktion"]] = df["text"].str.rsplit(n=1, pat="(", expand=True)
    df["funktion"] = df["funktion"].str[:-1]
    # URL erstellen
    df["url_adr"] = PATH_PERSONEN + df["uni_nr"]

    # Create url
    df["url_ratsmitgliedschaften"] = PATH_DATASET + "100307/?refine.uni_nr=" + df["uni_nr"]

    # append "name" and "vorname"
    df["name_vorname"] = df["name"] + ", " + df["vorname"]

    # Select relevant columns for publication
    cols_of_interest = [
        "rubrik",
        "intr-bind",
        "funktion",
        "text",
        "anrede",
        "name",
        "vorname",
        "name_vorname",
        "partei_kname",
        "url_adr",
        "uni_nr",
        "url_ratsmitgliedschaften",
    ]
    df = df[cols_of_interest]

    logging.info('Creating dataset "Grosser Rat: Interessensbindungen"...')
    path_export = os.path.join("data", "export", "100309_gr_interessensbindungen.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100309"


def create_gremien_csv(df_gre: pd.DataFrame, df_mit: pd.DataFrame) -> tuple:
    # To check which committees are currently active, we look at committees with current memberships
    # (with a 3-month buffer due to commissions sometimes lacking members for a while after a legislative period)
    unix_ts = (datetime.now() - datetime(1970, 4, 1)).total_seconds()
    df_mit["ist_aktuelles_gremium"] = df_mit["ende"].astype(int) > unix_ts

    df_mit = df_mit.groupby("uni_nr_gre").any("ist_aktuelles_gremium")
    df_mit["ist_aktuelles_gremium"] = np.where(df_mit["ist_aktuelles_gremium"], "Ja", "Nein")

    df = pd.merge(df_gre, df_mit, left_on="uni_nr", right_on="uni_nr_gre")

    # Create url's
    # URL for the committee's page (currently removed)
    # TODO: Add using Sitemap XML for current committees.
    # df['url_gre'] = credentials.path_gremium + df['uni_nr']
    df["url_mitgliedschaften"] = PATH_DATASET + "100308/?refine.uni_nr_gre=" + df["uni_nr"]
    df["url_urheber"] = PATH_DATASET + "100311/?refine.uni_nr_urheber=" + df["uni_nr"]
    df["url_zugew_geschaefte"] = PATH_DATASET + "100312/?refine.uni_nr_an=" + df["uni_nr"]

    # Select relevant columns for publication
    cols_of_interest = [
        "ist_aktuelles_gremium",
        "kurzname",
        "name",
        "gremientyp",
        "uni_nr",
        "url_mitgliedschaften",
        "url_urheber",
        "url_zugew_geschaefte",
    ]
    df = df[cols_of_interest]

    logging.info('Creating dataset "Grosser Rat: Gremien"...')
    path_export = os.path.join("data", "export", "100310_gr_gremien.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100310"


def create_geschaefte_csv(
    df_adr: pd.DataFrame,
    df_ges: pd.DataFrame,
    df_kon: pd.DataFrame,
    df_gre: pd.DataFrame,
) -> tuple:
    df = pd.merge(
        df_ges,
        df_adr,
        how="left",
        left_on="gr_urheber",
        right_on="uni_nr",
        suffixes=("_ges", "_adr"),
    )
    # Konsorten hinzufügen
    df = pd.merge(df, df_kon, how="left", left_on="laufnr", right_on="ges_laufnr")
    df = pd.merge(
        df,
        df_adr,
        how="left",
        left_on="uni_nr_adr",
        right_on="uni_nr",
        suffixes=("_urheber", "_miturheber"),
    )

    # Rename columns for clarity
    df = df.rename(
        columns={
            "beginn": "beginn_ges",
            "ende": "ende_ges",
            "laufnr": "laufnr_ges",
            "status": "status_ges",
            "signatur": "signatur_ges",
            "departement": "departement_ges",
            "gr_urheber": "nr_urheber",
            "uni_nr_adr": "nr_miturheber",
        }
    )

    # Create url's
    df["url_ges"] = PATH_GESCHAEFT + df["signatur_ges"]
    df["url_zuweisungen"] = PATH_DATASET + "100312/?refine.signatur_ges=" + df["signatur_ges"]
    df["url_dokumente"] = PATH_DATASET + "100313/?refine.signatur_ges=" + df["signatur_ges"]
    df["url_vorgaenge"] = PATH_DATASET + "100314/?refine.signatur_ges=" + df["signatur_ges"]

    # Replacing status codes with their meanings
    df["status_ges"] = df["status_ges"].replace(REPLACE_STATUS_CODES_GES)

    # Create url's for the urheber numbers, which are people (can also be gremium/commitee)
    df["url_urheber"] = np.where(df["vorname_urheber"].notna(), PATH_PERSONEN + df["nr_urheber"], np.nan)
    df["url_urheber_ratsmitgl"] = np.where(
        df["vorname_urheber"].notna(),
        PATH_DATASET + "100307/?refine.uni_nr=" + df["nr_urheber"],
        np.nan,
    )
    df["name_vorname_urheber"] = np.where(
        df["vorname_urheber"].notna(),
        df["name_urheber"] + ", " + df["vorname_urheber"],
        np.nan,
    )
    # Fields for names of person can be used for the committee as follows
    df.loc[df["vorname_urheber"].isna(), "gremientyp_urheber"] = df["nr_urheber"].map(
        df_gre.set_index("uni_nr")["gremientyp"]
    )
    df.loc[df["vorname_urheber"].isna(), "name_urheber"] = df["nr_urheber"].map(df_gre.set_index("uni_nr")["name"])
    df.loc[df["vorname_urheber"].isna(), "vorname_urheber"] = df["nr_urheber"].map(
        df_gre.set_index("uni_nr")["kurzname"]
    )
    # If name is still empty, add members from the json dict above
    df.loc[df["vorname_urheber"].isna(), "anrede_urheber"] = df["nr_urheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["anrede"]
    )
    df.loc[df["vorname_urheber"].isna(), "name_urheber"] = df["nr_urheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["name"]
    )
    df.loc[df["vorname_urheber"].isna(), "name_vorname_urheber"] = df["nr_urheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["name_vorname"]
    )
    df.loc[df["vorname_urheber"].isna(), "vorname_urheber"] = df["nr_urheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["vorname"]
    )

    # Similar approach for Miturheber
    df["url_miturheber"] = np.where(df["vorname_miturheber"].notna(), PATH_PERSONEN + df["nr_miturheber"], np.nan)
    df["url_miturheber_ratsmitgl"] = np.where(
        df["vorname_miturheber"].notna(),
        PATH_DATASET + "100307/?refine.uni_nr=" + df["nr_miturheber"],
        np.nan,
    )
    df["name_vorname_miturheber"] = np.where(
        df["vorname_miturheber"].notna(),
        df["name_miturheber"] + ", " + df["vorname_miturheber"],
        np.nan,
    )
    df.loc[df["vorname_miturheber"].isna(), "gremientyp_miturheber"] = df["nr_miturheber"].map(
        df_gre.set_index("uni_nr")["gremientyp"]
    )
    df.loc[df["vorname_miturheber"].isna(), "name_miturheber"] = df["nr_miturheber"].map(
        df_gre.set_index("uni_nr")["name"]
    )
    df.loc[df["vorname_miturheber"].isna(), "vorname_miturheber"] = df["nr_miturheber"].map(
        df_gre.set_index("uni_nr")["kurzname"]
    )
    # If name is still empty, add members from the json dict above
    df.loc[df["vorname_miturheber"].isna(), "anrede_miturheber"] = df["nr_miturheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["anrede"]
    )
    df.loc[df["vorname_miturheber"].isna(), "name_miturheber"] = df["nr_miturheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["name"]
    )
    df.loc[df["vorname_miturheber"].isna(), "name_vorname_miturheber"] = df["nr_miturheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["name_vorname"]
    )
    df.loc[df["vorname_miturheber"].isna(), "vorname_miturheber"] = df["nr_miturheber"].map(
        DF_MEMBERS_MISSING.set_index("uni_nr")["vorname"]
    )

    # Select relevant columns for publication
    cols_of_interest = [
        "beginn_ges",
        "ende_ges",
        "laufnr_ges",
        "signatur_ges",
        "status_ges",
        "titel_ges",
        "departement_ges",
        "ga_rr_gr",
        "url_ges",
        "url_zuweisungen",
        "url_dokumente",
        "url_vorgaenge",
        "anrede_urheber",
        "gremientyp_urheber",
        "name_urheber",
        "vorname_urheber",
        "name_vorname_urheber",
        "partei_kname_urheber",
        "url_urheber",
        "nr_urheber",
        "url_urheber_ratsmitgl",
        "anrede_miturheber",
        "gremientyp_miturheber",
        "name_miturheber",
        "vorname_miturheber",
        "name_vorname_miturheber",
        "partei_kname_miturheber",
        "url_miturheber",
        "nr_miturheber",
        "url_miturheber_ratsmitgl",
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ["beginn_ges", "ende_ges"])

    logging.info('Creating dataset "Grosser Rat: Geschäfte"...')
    path_export = os.path.join("data", "export", "100311_gr_geschaefte.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100311"


def create_zuweisungen_csv(df_gre: pd.DataFrame, df_ges: pd.DataFrame, df_zuw: pd.DataFrame) -> tuple:
    # All entries not present in gremium.csv are still inserted and treated as "Regierungsrat"
    df = pd.merge(df_gre, df_zuw, how="right", left_on="uni_nr", right_on="uni_nr_an")
    # Removing the column due to the following merging to avoid duplicate columns
    df = df.drop(["uni_nr"], axis=1)
    df = pd.merge(df, df_ges, left_on="ges_laufnr", right_on="laufnr", suffixes=("_zuw", "_ges"))
    df = pd.merge(
        df,
        df_gre,
        how="left",
        left_on="uni_nr_von",
        right_on="uni_nr",
        suffixes=("_an", "_von"),
    )

    # Rename columns for clarity
    df = df.rename(
        columns={
            "beginn": "beginn_ges",
            "ende": "ende_ges",
            "laufnr": "laufnr_ges",
            "status": "status_ges",
            "signatur": "signatur_ges",
            "departement": "departement_ges",
        }
    )

    # Create url's
    df["url_ges"] = PATH_GESCHAEFT + df["signatur_ges"]
    """ URL for committee's page (currently removed)
    df['url_gre_an'] = credentials.path_gremien + df['uni_nr_an']
    df['url_gre_von'] = credentials.path_gremien + df['uni_nr_von']
    """
    df["url_geschaeft_ods"] = PATH_DATASET + "100311/?refine.signatur_ges=" + df["signatur_ges"]
    df["url_gremium_an"] = np.where(
        df["name_an"].notna(),
        PATH_DATASET + "100310/?refine.uni_nr=" + df["uni_nr_an"],
        np.nan,
    )
    df["url_gremium_von"] = np.where(
        df["name_von"].notna(),
        PATH_DATASET + "100310/?refine.uni_nr=" + df["uni_nr_von"],
        np.nan,
    )

    # Temporarily replacing remaining committees not in committee list with "Regierungsrat" (without number)
    values = {
        "kurzname_an": "RR",
        "kurzname_von": "RR",
        "name_an": "Regierungsrat",
        "name_von": "Regierungsrat",
    }
    df = df.fillna(value=values)
    # Replacing status codes with their meanings
    df["status_zuw"] = df["status_zuw"].replace(REPLACE_STATUS_CODES_ZUW)
    df["status_ges"] = df["status_ges"].replace(REPLACE_STATUS_CODES_GES)

    # Select relevant columns for publication
    cols_of_interest = [
        "kurzname_an",
        "name_an",
        "uni_nr_an",
        "url_gremium_an",
        "erledigt",
        "status_zuw",
        "termin",
        "titel_zuw",
        "bem",
        "beginn_ges",
        "ende_ges",
        "laufnr_ges",
        "signatur_ges",
        "status_ges",
        "titel_ges",
        "ga_rr_gr",
        "departement_ges",
        "url_ges",
        "url_geschaeft_ods",
        "kurzname_von",
        "name_von",
        "uni_nr_von",
        "url_gremium_von",
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ["erledigt", "termin", "beginn_ges", "ende_ges"])

    logging.info('Creating dataset "Grosser Rat: Zuweisungen von Geschäften"...')
    path_export = os.path.join("data", "export", "100312_gr_zuweisungen.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100312"


def create_dokumente_csv(df_ges: pd.DataFrame, df_dok: pd.DataFrame) -> tuple:
    df = pd.merge(
        df_dok,
        df_ges,
        left_on="Laufnummer",
        right_on="laufnr",
        suffixes=("_dok", "_ges"),
    )

    # Rename columns for clarity
    df = df.rename(
        columns={
            "beginn": "beginn_ges",
            "ende": "ende_ges",
            "titel": "titel_ges",
            "laufnr": "laufnr_ges",
            "status": "status_ges",
            "signatur": "signatur_ges",
            "departement": "departement_ges",
            "Datum": "dokudatum",
            "Dokument Nr.": "dok_nr",
            "Url": "url",
            "Titel": "titel_dok",
            "Signatur": "signatur_dok",
        }
    )

    # Create url's
    df["url_ges"] = PATH_GESCHAEFT + df["signatur_ges"]
    df["url_geschaeft_ods"] = PATH_DATASET + "100311/?refine.signatur_ges=" + df["signatur_ges"]

    def build_url(signatur):
        if pd.isna(signatur):
            return None
        parts = str(signatur).split()
        if len(parts) == 1:  # single value
            return PATH_DOKUMENT + parts[0]
        return None  # fallback to df["url"]

    df["url_dok"] = np.where(
        df["signatur_dok"].notna(), df["signatur_dok"].apply(build_url).fillna(df["url"]), df["url"]
    )

    # Replacing status codes with their meanings
    df["status_ges"] = df["status_ges"].replace(REPLACE_STATUS_CODES_GES)

    # Select relevant columns for publication
    cols_of_interest = [
        "dokudatum",
        "dok_nr",
        "titel_dok",
        "url_dok",
        "signatur_dok",
        "beginn_ges",
        "ende_ges",
        "laufnr_ges",
        "signatur_ges",
        "status_ges",
        "titel_ges",
        "ga_rr_gr",
        "departement_ges",
        "url_ges",
        "url_geschaeft_ods",
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df["dokudatum"] = pd.to_datetime(df["dokudatum"], format="%d.%m.%Y", errors="coerce")
    df = unix_to_datetime(df, ["beginn_ges", "ende_ges"])

    # Temporarily
    df = df.rename(columns={"dok_nr": "dok_laufnr"})

    logging.info('Creating dataset "Grosser Rat: Dokumente"...')
    path_export = os.path.join("data", "export", "100313_gr_dokumente.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100313"


def create_vorgaenge_csv(df_ges: pd.DataFrame, df_vor: pd.DataFrame, df_siz: pd.DataFrame) -> tuple:
    df = pd.merge(df_vor, df_ges, left_on="ges_laufnr", right_on="laufnr")
    df = pd.merge(df, df_siz, on="siz_nr")

    # Rename columns for clarity
    df = df.rename(
        columns={
            "beginn": "beginn_ges",
            "ende": "ende_ges",
            "laufnr": "laufnr_ges",
            "status": "status_ges",
            "signatur": "signatur_ges",
            "departement": "departement_ges",
            "titel": "titel_ges",
            "datum": "siz_datum",
        }
    )

    # Create url's
    df["url_ges"] = PATH_GESCHAEFT + df["signatur_ges"]
    df["url_geschaeft_ods"] = PATH_DATASET + "100311/?refine.signatur_ges=" + df["signatur_ges"]

    # Replacing status codes with their meanings
    df["status_ges"] = df["status_ges"].replace(REPLACE_STATUS_CODES_GES)

    # Select relevant columns for publication
    cols_of_interest = [
        "beschlnr",
        "nummer",
        "Vermerk",
        "siz_nr",
        "siz_datum",
        "beginn_ges",
        "ende_ges",
        "laufnr_ges",
        "signatur_ges",
        "status_ges",
        "titel_ges",
        "ga_rr_gr",
        "departement_ges",
        "url_ges",
        "url_geschaeft_ods",
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ["siz_datum", "beginn_ges", "ende_ges"])

    logging.info('Creating dataset "Grosser Rat: Vorgänge von Geschäften"...')
    path_export = os.path.join("data", "export", "100314_gr_vorgaenge.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100314"


def create_traktanden_csv(
    df_gr_tagesordnung: pd.DataFrame,
    df_gr_traktanden: pd.DataFrame,
    df_gr_sitzung: pd.DataFrame,
) -> tuple:
    transformed_columns = [
        "idnr",
        "gr_sitzung_idnr",
        "einleitungstext",
        "zwischentext",
        "gruppennummer",
        "gruppentitel",
        "gruppentitel_pos",
        "gruppentitel_next_pos",
    ]
    transformed_df = pd.DataFrame(columns=transformed_columns)

    # Finding out how many gruppentitel there are
    max_gruppentitel = max(int(col.split("_")[1]) for col in df_gr_tagesordnung.columns if "gruppentitel_" in col)

    # Transforming the dataframe to have one row per gruppentitel
    for index, row in df_gr_tagesordnung.iterrows():
        for i in range(1, max_gruppentitel + 1):
            gruppentitel = row[f"gruppentitel_{i}"]
            gruppentitel_pos = row[f"gruppentitel_{i}_pos"]
            gruppentitel_next_pos = row[f"gruppentitel_{i + 1}_pos"] if i < max_gruppentitel else "0"

            # Skip rows where gruppentitel is empty
            if pd.notna(gruppentitel) and gruppentitel != "":
                new_row = {
                    "idnr": row["idnr"],
                    "gr_sitzung_idnr": row["gr_sitzung_idnr"],
                    "einleitungstext": row["einleitungstext"],
                    "zwischentext": row["zwischentext"],
                    "gruppennummer": i,
                    "gruppentitel": gruppentitel,
                    "gruppentitel_pos": gruppentitel_pos,
                    "gruppentitel_next_pos": gruppentitel_next_pos,
                }
                transformed_df = pd.concat([transformed_df, pd.DataFrame([new_row])], ignore_index=True)
    # Replace 0 in gruppentitel_next_pos with a big number
    transformed_df["gruppentitel_next_pos"] = transformed_df["gruppentitel_next_pos"].replace(
        "0", np.nan
    )  # last group or missing next
    transformed_df["gruppentitel_pos"] = pd.to_numeric(transformed_df["gruppentitel_pos"], errors="coerce")
    transformed_df["gruppentitel_next_pos"] = pd.to_numeric(
        transformed_df["gruppentitel_next_pos"], errors="coerce"
    ).fillna(999999)

    df = pd.merge(transformed_df, df_gr_sitzung, left_on="gr_sitzung_idnr", right_on="idnr")
    df = df.drop(columns=["idnr_y"]).rename(columns={"idnr_x": "idnr"})

    df_merge1 = pd.merge(df, df_gr_traktanden, left_on="idnr", right_on="tagesordnung_idnr")
    df_merge1 = df_merge1.drop(columns=["idnr_x"]).rename(columns={"idnr_y": "traktanden_idnr"})

    # laufnr must be greater than or equal to gruppentitel_pos and
    # less than gruppentitel_next_pos,
    condition = (df_merge1["laufnr"].astype(int) >= df_merge1["gruppentitel_pos"].astype(int)) & (
        df_merge1["laufnr"].astype(int) < df_merge1["gruppentitel_next_pos"].astype(int)
    )
    df_merge1 = df_merge1[condition].reset_index(drop=True)

    # Some do not belong to a group at all. Add them with the gruppentitel-columns empty
    df_trakt_missing = df_gr_traktanden[~df_gr_traktanden["idnr"].isin(df_merge1["traktanden_idnr"])]
    df_to_merge = df[
        [
            "idnr",
            "gr_sitzung_idnr",
            "versand",
            "tag1",
            "text1",
            "tag2",
            "text2",
            "tag3",
            "text3",
            "bemerkung",
            "protokollseite_von",
            "protokollseite_bis",
            "einleitungstext",
            "zwischentext",
        ]
    ].drop_duplicates()
    df_merge2 = pd.merge(
        df_to_merge,
        df_trakt_missing,
        left_on="idnr",
        right_on="tagesordnung_idnr",
        how="right",
    )
    df_merge2["gruppennummer"] = 0
    df_merge2["gruppentitel"] = pd.NA
    df_merge2["gruppentitel_pos"] = pd.NA
    df_merge2 = df_merge2.drop(columns=["idnr_x"]).rename(columns={"idnr_y": "traktanden_idnr"})

    df = pd.concat([df_merge1, df_merge2], ignore_index=True)

    # Replace 0000-00-00 in columns tag1 until tag3 with NaN
    df.loc[:, "tag1":"tag3"] = df.loc[:, "tag1":"tag3"].replace("0000-00-00", np.nan)

    # Replace status with their corresponding text
    df["status"] = df["status"].replace(REPLACE_STATUS_CODES_TRAKT)

    # Extend Abstimmung into several columns:
    def safe_literal_eval(val):
        try:
            return ast.literal_eval(val)
        except (ValueError, SyntaxError):
            return np.nan

    df["anr"] = df["Abstimmung"].str.replace(".pdf", "").apply(safe_literal_eval)
    df["anr"] = (
        pd.json_normalize(df["anr"]).map(transform_value).apply(lambda x: ",".join(x.dropna().astype(str)), axis=1)
    )

    # Create url's
    df["url_tagesordnung_dok"] = PATH_MEDIA_TAGESORDNUNG + "tagesordnung_" + df["tag1"] + ".pdf"
    df["url_geschaeftsverzeichnis"] = PATH_MEDIA_TAGESORDNUNG + "geschaeftsverzeichnis_" + df["tag1"] + ".pdf"
    df.loc[pd.to_datetime(df["tag1"]) > pd.to_datetime("2014-06-01"), "url_sammelmappe"] = (
        PATH_MEDIA_TAGESORDNUNG + "sammelmappe_to_" + df["tag1"] + ".pdf"
    )
    df.loc[pd.to_datetime(df["tag1"]) > pd.to_datetime("2014-06-01"), "url_alle_dokumente"] = (
        PATH_MEDIA_TAGESORDNUNG + "alle_dokumente_to_" + df["tag1"] + ".zip"
    )
    df.loc[pd.to_datetime(df["tag1"]) < pd.to_datetime("2024-01-01"), "url_vollprotokoll"] = (
        PATH_MEDIA_RATSPROTOKOLLE + "vollprotokoll_" + df["tag1"] + ".pdf"
    )
    # Only the system before 2023-07-01 has this URL for the audio and video protocols
    for tag in ["tag1", "tag2", "tag3"]:
        df.loc[
            pd.to_datetime(df[tag]) < pd.to_datetime("2023-07-01"),
            f"url_audioprotokoll_{tag}",
        ] = PATH_MEDIA_AUDIO + "?sitzung=" + df[tag]
    df["url_kommission"] = PATH_DATASET + "100310/?refine.kurzname=" + df["kommission"]
    df["url_ges"] = ""
    df["url_geschaeft_ods"] = ""
    df["url_dok"] = ""
    df["url_dokument_ods"] = ""
    df["url_abstimmungen"] = ""
    for index, row in df.iterrows():
        if isinstance(row["signatur"], str) and "." in row["signatur"]:
            df.at[index, "url_ges"] = ""
            df.at[index, "url_geschaeft_ods"] = f"{PATH_DATASET}100311/?"
            df.at[index, "url_dok"] = ""
            df.at[index, "url_dokument_ods"] = f"{PATH_DATASET}100313/?"
            signaturen = row["signatur"].split(",")
            for signatur in signaturen:
                signatur_parts = signatur.split(".")
                df.at[index, "url_ges"] += f"{PATH_GESCHAEFT}{'.'.join(signatur_parts[:2])} ,"
                df.at[index, "url_geschaeft_ods"] += f"refine.signatur_ges={'.'.join(signatur_parts[:2])}&"
                df.at[index, "url_dok"] += f"{PATH_DOKUMENT}{signatur} ,"
                df.at[index, "url_dokument_ods"] += f"refine.signatur_dok={signatur}&"
            # remove last char of every url
            df.at[index, "url_ges"] = df.at[index, "url_ges"][:-1]
            df.at[index, "url_geschaeft_ods"] = df.at[index, "url_geschaeft_ods"][:-1]
            df.at[index, "url_dok"] = df.at[index, "url_dok"][:-1]
            df.at[index, "url_dokument_ods"] = df.at[index, "url_dokument_ods"][:-1]
        if isinstance(row["anr"], str) and "-" in row["anr"]:
            df.at[index, "url_abstimmungen"] = f"{PATH_DATASET}100186/?"
            anrs = row["anr"].split(",")
            for anr in anrs:
                df.at[index, "url_abstimmungen"] += f"refine.anr={anr}&"
            # remove last char of url
            df.at[index, "url_abstimmungen"] = df.at[index, "url_abstimmungen"][:-1]

    # Select relevant columns for publication
    cols_of_interest = [
        "gr_sitzung_idnr",
        "versand",
        "tag1",
        "text1",
        "tag2",
        "text2",
        "tag3",
        "text3",
        "bemerkung",
        "protokollseite_von",
        "protokollseite_bis",
        "url_vollprotokoll",
        "url_audioprotokoll_tag1",
        "url_audioprotokoll_tag2",
        "url_audioprotokoll_tag3",
        "tagesordnung_idnr",
        "einleitungstext",
        "zwischentext",
        "url_tagesordnung_dok",
        "url_geschaeftsverzeichnis",
        "url_sammelmappe",
        "url_alle_dokumente",
        "gruppennummer",
        "gruppentitel",
        "gruppentitel_pos",
        "traktanden_idnr",
        "laufnr",
        "laufnr_2",
        "status",
        "titel",
        "kommission",
        "url_kommission",
        "departement",
        "signatur",
        "url_ges",
        "url_geschaeft_ods",
        "url_dok",
        "url_dokument_ods",
        "Abstimmung",
        "anr",
        "url_abstimmungen",
    ]

    df = df[cols_of_interest]

    logging.info('Creating dataset "Grosser Rat: Traktanden"...')
    path_export = os.path.join("data", "export", "100348_gr_traktanden.csv")
    df.to_csv(path_export, index=False)
    # Returning the path where the created CSV-file is stored
    # and two string identifiers which are needed to update the file in the FTP server and in ODSP
    return path_export, "parlamentsdienst/grosser_rat", "100348"


# Transform Abstimmungs-IDS from GRBS-Abst-YYYYMMDD-HHMMSS-TXXX-YY-ZZZZZZZ to ZZZZZZZ-YYYYMMDD-HHMMSS
def transform_value(value):
    if pd.isnull(value):
        return value  # Return NaN as is
    parts = value.split("-")
    return f"{parts[-1]}-{parts[2]}-{parts[3]}"


def unix_to_datetime(df: pd.DataFrame, column_names: list) -> pd.DataFrame:
    """
    Converts Unix timestamps in specified columns of a DataFrame to datetime format.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        column_names (list): List of column names containing Unix timestamps.

    Returns:
        pd.DataFrame: DataFrame with converted datetime values.
    """
    # Replace '0' values with NaN to handle missing timestamps
    df[column_names] = df[column_names].replace("0", np.nan)

    # Loop through each specified column and convert Unix timestamps to formatted datetime strings
    for column_name in column_names:
        df[column_name] = pd.to_datetime(df[column_name].astype(float), unit="s", errors="coerce")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful")
