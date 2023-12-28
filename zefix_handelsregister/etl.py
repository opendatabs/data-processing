import os
import pandas as pd
import numpy as np
import requests
import logging
import pathlib

import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp
from zefix_handelsregister import credentials
from SPARQLWrapper import SPARQLWrapper, JSON
import urllib.request

proxy_support = urllib.request.ProxyHandler(common.credentials.proxies)
opener = urllib.request.build_opener(proxy_support)
urllib.request.install_opener(opener)


def main():
    # Get nomenclature data from i14y.admin.ch
    dfs_nomenclature_noga = {}
    for i in range(1, 6):
        dfs_nomenclature_noga[i] = get_noga_nomenclature(i)
    # Get Zefix and BurWeb data for all cantons
    for lang in ('de', 'fr'):
        get_data_of_all_cantons(dfs_nomenclature_noga, lang)

    # Extract data for Basel-Stadt and make ready for data.bs.ch
    file_name = '100330_zefix_firmen_BS.csv'
    path_export = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'noga_nomenclature', file_name)
    df_BS = work_with_BS_data()
    df_BS.to_csv(path_export, index=False)
    if ct.has_changed(path_export):
        logging.info(f'Exporting {file_name} to FTP server')
        common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'zefix_handelsregister')
        odsp.publish_ods_dataset_by_id('100330')
        ct.update_hash_file(path_export)


def get_noga_nomenclature(level):
    url_noga = f'https://www.i14y.admin.ch/api/Nomenclatures/HCL_NOGA/levelexport/CSV?level={level}'
    r = common.requests_get(url_noga)
    path_nomenclature = os.path.join(pathlib.Path(__file__).parents[0], 'data', f'nomenclature_noga_lv{level}.csv')
    with open(path_nomenclature, 'wb') as f:
        f.write(r.content)
    df_nomenclature_noga = pd.read_csv(path_nomenclature, dtype=str)
    return df_nomenclature_noga


def get_noga_data(df, dfs_nomenclature_noga, lang):
    # Expand by noga columns
    df[['noga_abschnitt_code', 'noga_abschnitt', 'noga_abteilung_code', 'noga_abteilung',
        'noga_gruppe_code', 'noga_gruppe', 'noga_klasse_code', 'noga_klasse', 'noga_code', 'noga']] = np.nan
    # Iterate over all rows with iterrows()
    for index, row in df.iterrows():
        uid = row['company_uid']
        url = f'https://www.burweb2.admin.ch/BurWeb.Services.External/api/v1/Enterprise/{uid}/NogaCode'

        try:
            r = common.requests_get(url, auth=(credentials.user_burweb, credentials.pass_burweb))
            r.raise_for_status()
            row['noga_code'] = r.json().get('NogaCode', 'Unknown')
        except requests.RequestException as e:
            print(f"Error fetching data for UID {uid}: {e}")
            return df

        # Additional logic to fetch and return more data can be added here
        row['noga_abteilung_code'] = row['noga_code'][:2]
        row['noga_abschnitt_code'] = dfs_nomenclature_noga[2].loc[dfs_nomenclature_noga[2]['Code'] == row['noga_abteilung_code'], 'Parent'].values[0]
        row['noga_abschnitt'] = dfs_nomenclature_noga[1].loc[dfs_nomenclature_noga[1]['Code'] == row['noga_abschnitt_code'], f'Name_{lang}'].values[0]
        row['noga_abteilung'] = dfs_nomenclature_noga[2].loc[dfs_nomenclature_noga[2]['Code'] == row['noga_abteilung_code'], f'Name_{lang}'].values[0]
        row['noga_gruppe_code'] = row['noga_code'][:3]
        row['noga_gruppe'] = dfs_nomenclature_noga[3].loc[dfs_nomenclature_noga[3]['Code'] == row['noga_gruppe_code'], f'Name_{lang}'].values[0]
        row['noga_klasse_code'] = row['noga_code'][:4]
        row['noga_klasse'] = dfs_nomenclature_noga[4].loc[dfs_nomenclature_noga[4]['Code'] == row['noga_klasse_code'], f'Name_{lang}'].values[0]
        row['noga'] = dfs_nomenclature_noga[5].loc[dfs_nomenclature_noga[5]['Code'] == row['noga_code'], f'Name_{lang}'].values[0]
        df.loc[index] = row
    return df

def get_data_of_all_cantons(dfs_nomenclature_noga, lang):
    sparql = SPARQLWrapper("https://lindas.admin.ch/query")
    sparql.setReturnFormat(JSON)
    # Iterate over all cantons
    for i in range(1, 27):
        logging.info(f'Getting data for canton {i} in language {lang}...')
        # Query can be tested and adjusted here: https://ld.admin.ch/sparql/#
        sparql.setQuery("""
                PREFIX schema: <http://schema.org/>
                PREFIX admin: <https://schema.ld.admin.ch/>
                SELECT ?canton_id ?canton ?short_name_canton ?district_id ?district ?muni_id ?municipality ?company_uri ?company_uid ?company_legal_name ?type_id ?company_type ?adresse ?plz ?locality 
                WHERE {
                    # Get information of the company
                    ?company_uri a admin:ZefixOrganisation ;
                        schema:legalName ?company_legal_name ;
                        admin:municipality ?muni_id ;
                        schema:identifier ?company_identifiers ;
                        schema:address ?adr ;
                        schema:additionalType ?type_id .
                    # Get Identifier UID, but filter by CompanyUID, since there are three types of ID's
                    ?company_identifiers schema:value ?company_uid .
                    ?company_identifiers schema:name "CompanyUID" .
                    ?muni_id schema:name ?municipality .
                    ?type_id schema:name ?company_type .
                    # Get address-information (do not take c/o-information in, since we get fewer results)
                    ?adr schema:streetAddress ?adresse ;
                        schema:addressLocality ?locality ;
                        schema:postalCode ?plz .
                    # Finally filter by Companies that are in a certain canton
                    <https://ld.admin.ch/canton/""" + str(i) + """> schema:containsPlace ?muni_id ;
                        schema:legalName ?canton ;
                        schema:alternateName ?short_name_canton ;
                        schema:identifier ?canton_id .
                    ?district_id schema:containsPlace ?muni_id ;
                        schema:name ?district .

                  # Filter by company-types that are german (otherwise result is much bigger)
                  FILTER langMatches(lang(?district), \"""" + lang + """\") .
                  FILTER langMatches(lang(?company_type), \"""" + lang + """\") .
                }
                ORDER BY ?company_legal_name
            """)

        results = sparql.query().convert()
        results_df = pd.json_normalize(results['results']['bindings'])
        results_df = results_df.filter(regex='value$', axis=1)
        new_column_names = {col: col.replace('.value', '') for col in results_df.columns}
        results_df = results_df.rename(columns=new_column_names)
        # Split the column 'address' into zusatz and street,
        # but if there is no zusatz, then street is in the first column
        results_df.loc[results_df['adresse'].str.contains('\n'), ['zusatz', 'street']] = results_df[
            'adresse'].str.split('\n', expand=True)
        results_df.loc[~results_df['adresse'].str.contains('\n'), 'street'] = results_df['adresse']
        results_df = results_df.drop(columns=['adresse'])

        # Get noga data
        results_df = get_noga_data(results_df, dfs_nomenclature_noga, lang)

        file_name = f"companies_{results_df['short_name_canton'][0]}_{lang}.csv"
        path_export = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'all_cantons', file_name)
        results_df.to_csv(path_export, index=False)
        if ct.has_changed(path_export):
            logging.info(f'Exporting {file_name} to FTP server')
            common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              f'zefix_handelsregister/all_cantons')
            ct.update_hash_file(path_export)


def get_gebaeudeeingaenge():
    raw_data_file = os.path.join(pathlib.Path(__file__).parent, 'data', 'gebaeudeeingaenge.csv')
    logging.info(f'Downloading Gebäudeeingänge from ods to file {raw_data_file}...')
    r = common.requests_get(f'https://data.bs.ch/api/records/1.0/download?dataset=100231')
    with open(raw_data_file, 'wb') as f:
        f.write(r.content)
    return raw_data_file


def work_with_BS_data():
    path_BS = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'all_cantons', 'companies_BS_de.csv')
    df_BS = pd.read_csv(path_BS)
    # Replace *Str.* with *Strasse* and *str.* with *strasse*
    df_BS['street'] = df_BS['street'].str.replace('Str.', 'Strasse')
    df_BS['street'] = df_BS['street'].str.replace('str.', 'strasse')
    path_geb_eing = get_gebaeudeeingaenge()
    df_geb_eing = pd.read_csv(path_geb_eing, sep=';')
    df_geb_eing['street'] = df_geb_eing['strname'] + ' ' + df_geb_eing['deinr'].astype(str)
    # Merge on street
    df_merged = pd.merge(df_BS, df_geb_eing, on='street', how='left')
    return df_merged[['company_type', 'type_id', 'municipality', 'locality', 'canton_id',
                      'company_legal_name', 'short_name_canton', 'district', 'company_uid',
                      'canton', 'muni_id', 'district_id', 'company_uri', 'plz', 'zusatz',
                      'street', 'egid', 'eingang_koordinaten', 'noga_abschnitt_code',
                      'noga_abschnitt', 'noga_abteilung_code', 'noga_abteilung',
                      'noga_gruppe_code', 'noga_gruppe', 'noga_klasse_code',
                      'noga_klasse', 'noga_code', 'noga']]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful')
