import os
import pandas as pd
import numpy as np
import logging
import pathlib

import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp
from zefix_handelsregister import credentials
from SPARQLWrapper import SPARQLWrapper, JSON
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


def main():
    get_data_of_all_cantons()
    # TODO: Add funtion to extract the data from BS


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
                                   'data', 'export', file_name)
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
