import logging
import os

import common
import pandas as pd
import requests
from dotenv import load_dotenv
from requests_ntlm import HttpNtlmAuth

load_dotenv()

URL_LEISTUNGEN = os.getenv("URL_LEISTUNGEN")
URL_GEBUEHREN = os.getenv("URL_GEBUEHREN")
HOST_KLV = os.getenv("HOST_KLV")
API_USER_KLV = os.getenv("API_USER_KLV")
API_PASS_KLV = os.getenv("API_PASS_KLV")


def main():
    df_leist = get_leistungen()
    path_leist = os.path.join("data", "export", "leistungen.csv")
    df_leist.to_csv(path_leist, index=False)

    df_geb = get_gebuehren()
    path_geb = os.path.join("data", "export", "gebuehren.csv")
    df_geb.to_csv(path_geb, index=False)

    common.update_ftp_and_odsp(path_leist, "klv", "100324")
    common.update_ftp_and_odsp(path_geb, "klv", "100325")


def get_leistungen():
    req = common.requests_get(
        URL_LEISTUNGEN,
        auth=HttpNtlmAuth(API_USER_KLV, API_PASS_KLV),
        headers={"host": HOST_KLV},
        verify=False,
    )
    all_leistungen_path = os.path.join("data_orig", "alle_Leistungen.xlsx")
    open(all_leistungen_path, "wb").write(req.content)

    df_leist = pd.read_excel(all_leistungen_path, engine="openpyxl")
    df_leist = df_leist[df_leist["Aktiv"] == "Aktiv"]
    # TODO: keine == Keine == NaN alles zu Keine machen
    columns_of_interest = [
        "LeistungId",
        "Aktiv",
        "Departement",
        "Diensstelle",
        "Weitere Gliederung OE",
        "Identifikationsnummer",
        "Name",
        "Ergebnis",
        "Beschreibung",
        "Strasse",
        "Hausnummer",
        "Postleitzahl",
        "Ort",
        "Empfänger der Leistung",
        "Aktivität Leistungserbringer",
        "Aktivität Leistungsempfänger",
        "Vorbedingungen",
        "Rechtliche Grundlage",
        "Digitalisierungsgrad",
        "Kurzbeschrieb Ablauf",
        "Kontaktaufnahme via",
        "Frist",
        "Dauer",
        "Erforderliche Dokumente",
        "Formulare",
        "elektr. Bezahlmöglichkeit",
        "Weitere beteiligte Stellen",
        "Gebühren",
        "DepartementId",
        "DienststelleId",
        "Web Adresse",
        "Schlagworte",
    ]
    return df_leist[columns_of_interest]


def get_gebuehren():
    req = common.requests_get(
        URL_GEBUEHREN,
        auth=HttpNtlmAuth(API_USER_KLV, API_PASS_KLV),
        headers={"host": HOST_KLV},
        verify=False,
    )
    all_gebuehren_path = os.path.join("data_orig", "alle_aktiven_Gebuehren.xlsx")
    open(all_gebuehren_path, "wb").write(req.content)

    df_geb = pd.read_excel(all_gebuehren_path, engine="openpyxl")
    columns_of_interest = [
        "Diensstelle",
        "Gegenstand der Gebühr",
        "Rechtliche Grundlage",
        "Höhe der Gebühr(en) CHF",
        "Benchmark",
        "Leistung",
        "Departement",
        "WeitereGliederungOE",
    ]
    return df_geb[columns_of_interest]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
