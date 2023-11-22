import os
import pandas as pd
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
    get_data_of_all_cantons()
    file_name = '100330_zefix_firmen_BS.csv'
    path_export = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'export', file_name)
    df_BS = work_with_BS_data()
    df_BS.to_csv(path_export, index=False)
    if ct.has_changed(path_export):
        logging.info(f'Exporting {file_name} to FTP server')
        common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'zefix_handelsregister')
        ct.update_hash_file(path_export)


def get_gebaeudeeingaenge():
    raw_data_file = os.path.join(pathlib.Path(__file__).parent, 'data', 'gebaeudeeingaenge.csv')
    logging.info(f'Downloading Gebäudeeingänge from ods to file {raw_data_file}...')
    r = common.requests_get(f'https://data.bs.ch/api/records/1.0/download?dataset=100231')
    with open(raw_data_file, 'wb') as f:
        f.write(r.content)
    return raw_data_file


def work_with_BS_data():
    path_BS = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'all_cantons', 'companies_BS.csv')
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
                      'street', 'egid', 'eingang_koordinaten']]


def get_data_of_all_cantons():
    sparql = SPARQLWrapper("https://lindas.admin.ch/query")
    sparql.setReturnFormat(JSON)
    # Iterate over all cantons
    for i in range(1, 27):
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
                  FILTER langMatches(lang(?district), "de") .
                  FILTER langMatches(lang(?company_type), "de") .
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
        results_df.loc[results_df['adresse'].str.contains('\n'), ['zusatz', 'street']] = results_df['adresse'].str.split('\n', expand=True)
        results_df.loc[~results_df['adresse'].str.contains('\n'), 'street'] = results_df['adresse']
        results_df = results_df.drop(columns=['adresse'])

        # TODO: Add NOGA-data by accessing the BurWeb-API
        file_name = 'companies_' + results_df['short_name_canton'][0] + '.csv'
        path_export = os.path.join(pathlib.Path(__file__).parents[0],
                                   'data', 'all_cantons', file_name)
        results_df.to_csv(path_export, index=False)
        if ct.has_changed(path_export):
            logging.info(f'Exporting {file_name} to FTP server')
            common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                              f'zefix_handelsregister/all_cantons')
            ct.update_hash_file(path_export)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful')
