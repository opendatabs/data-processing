import os
import logging
import pandas as pd
import requests
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp

from requests_ntlm import HttpNtlmAuth
from itbs_klv import credentials


def main():
    df_leist = get_leistungen()
    path_leist = os.path.join(credentials.data_path, 'export', 'leistungen.csv')
    df_leist.to_csv(path_leist, index=False)

    df_geb = get_gebuehren()
    path_geb = os.path.join(credentials.data_path, 'export', 'gebuehren.csv')
    df_geb.to_csv(path_geb, index=False)

    if ct.has_changed(path_leist):
        common.upload_ftp(path_leist, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'klv')
        odsp.publish_ods_dataset_by_id('100324')
        ct.update_hash_file(path_leist)

    if ct.has_changed(path_geb):
        common.upload_ftp(path_geb, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'klv')
        odsp.publish_ods_dataset_by_id('100325')
        ct.update_hash_file(path_geb)


def get_leistungen():
    req = requests.get(credentials.url_leistungen, auth=HttpNtlmAuth(credentials.api_user, credentials.api_pass), verify=False)
    all_leistungen_path = os.path.join(credentials.data_orig_path, 'alle_Leistungen.xlsx')
    open(all_leistungen_path, 'wb').write(req.content)

    df_leist = pd.read_excel(all_leistungen_path)
    to_remove = ['Freizugeben durch', 'Freizugeben durch AD', 'Freigegeben am', 'Freigegeben durch',
                 'OE', 'Weitere Gliederung', 'Verantwortlicher Vorname', 'Verantwortlicher Name', 'Strasse',
                 'Hausnummer', 'Postleitzahl', 'Ort', 'Web Adresse', 'Email', 'Telefon', 'AKV', 'Grafische Darstellung',
                 'Zuordnung Prozesslandkarte', 'Anwendungen', 'Vorhaben', 'Weiterführende Infos', 'Weitere Angaben',
                 'Katastrophenbewältigung', 'Personendaten', 'AenderungDurch', 'AenderungAm']
    return df_leist.drop(columns=to_remove)


def get_gebuehren():
    req = requests.get(credentials.url_gebuehren, auth=HttpNtlmAuth(credentials.api_user, credentials.api_pass), verify=False)
    all_gebuehren_path = os.path.join(credentials.data_orig_path, 'alle_aktiven_Gebuehren.xlsx')
    open(all_gebuehren_path, 'wb').write(req.content)

    df_geb = pd.read_excel(all_gebuehren_path)
    to_remove = ['Kalkulationsgrundlage', 'Gesamteinkommen CHF/Jahr', 'SAP Konto 1', 
                 'SAP Konto 2', 'SAP Konto 3', 'Weitere Angaben zur Gebühr']
    return df_geb.drop(columns=to_remove)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
