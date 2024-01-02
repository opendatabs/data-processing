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
    # Get NOGA data (Temporarily deactivated)
    # df_burweb = get_noga_data()
    # Get Zefix and BurWeb data for all cantons
    get_data_of_all_cantons()

    # Extract data for Basel-Stadt and make ready for data.bs.ch
    file_name = '100330_zefix_firmen_BS.csv'
    path_export = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'export', file_name)
    df_BS = work_with_BS_data()
    df_BS.to_csv(path_export, index=False)
    if ct.has_changed(path_export):
        logging.info(f'Exporting {file_name} to FTP server')
        common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'zefix_handelsregister')
        odsp.publish_ods_dataset_by_id('100330')
        ct.update_hash_file(path_export)


def get_data_of_all_cantons():
    sparql = SPARQLWrapper("https://lindas.admin.ch/query")
    sparql.setReturnFormat(JSON)
    # Iterate over all cantons
    for i in range(12, 13):
        logging.info(f'Getting data for canton {i}...')
        # Query can be tested and adjusted here: https://ld.admin.ch/sparql/#
        sparql.setQuery("""
                PREFIX schema: <http://schema.org/>
                PREFIX admin: <https://schema.ld.admin.ch/>
                SELECT ?canton_id ?canton ?short_name_canton ?district_id ?district_de ?district_fr ?district_it ?district_en ?muni_id ?municipality ?company_uri ?company_uid ?company_legal_name ?type_id ?company_type_de ?company_type_fr ?adresse ?plz ?locality 
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
                    ?type_id schema:name ?company_type_de .
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
                        schema:name ?district_de .
  
                    # Optional to get district names in French
                    OPTIONAL {
                        ?district_id schema:containsPlace ?muni_id ;
                            schema:name ?district_fr .
                        FILTER langMatches(lang(?district_fr), "fr")
                    }
                    
                    # Optional to get district names in Italian
                    OPTIONAL {
                        ?district_id schema:containsPlace ?muni_id ;
                            schema:name ?district_it .
                        FILTER langMatches(lang(?district_it), "it")
                    }
                    
                    # Optional to get district names in English
                    OPTIONAL {
                        ?district_id schema:containsPlace ?muni_id ;
                            schema:name ?district_en .
                        FILTER langMatches(lang(?district_en), "en")
                    }
                  
                    # Optional to get company types in French
                    OPTIONAL {
                        ?type_id schema:name ?company_type_fr .
                        FILTER langMatches(lang(?company_type_fr), "fr")
                    }
                  
                    # Filter by company-types that are german (otherwise result is much bigger)
                    FILTER langMatches(lang(?district_de), "de") .
                    FILTER langMatches(lang(?company_type_de), "de") .
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
        temp_df = results_df['adresse'].str.split('\n', expand=True)
        results_df.loc[results_df['adresse'].str.contains('\n'), 'zusatz'] = temp_df[0]
        results_df.loc[results_df['adresse'].str.contains('\n'), 'street'] = temp_df[1]
        results_df.loc[~results_df['adresse'].str.contains('\n'), 'street'] = temp_df[0]
        results_df = results_df.drop(columns=['adresse'])

        '''
        # Get noga data
        results_df = pd.merge(results_df, df_burweb, on='company_uid', how='left')
        '''

        file_name = f"companies_{results_df['short_name_canton'][0]}.csv"
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
    path_BS = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'all_cantons', 'companies_BS.csv')
    df_BS = pd.read_csv(path_BS)
    # Replace *Str.* with *Strasse* and *str.* with *strasse*
    df_BS['street'] = df_BS['street'].str.replace('Str.', 'Strasse', regex=False)
    df_BS['street'] = df_BS['street'].str.replace('str.', 'strasse', regex=False)
    # Replace *St. followed by a letter with *St. * and then the letter
    df_BS['street'] = df_BS['street'].str.replace('St.([a-zA-Z])', 'St. \\1', regex=True)
    path_geb_eing = get_gebaeudeeingaenge()
    df_geb_eing = pd.read_csv(path_geb_eing, sep=';')
    df_geb_eing['street'] = df_geb_eing['strname'] + ' ' + df_geb_eing['deinr'].astype(str)
    # Merge on street
    df_merged = pd.merge(df_BS, df_geb_eing, on='street', how='left')
    return df_merged[['company_type_de', 'type_id', 'municipality', 'locality',
                      'company_legal_name', 'company_uid', 'muni_id', 'company_uri', 'plz', 'zusatz',
                      'street', 'egid', 'eingang_koordinaten']]


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful')


# Temporarily not needed
'''
# https://stackoverflow.com/questions/6999565/python-https-get-with-basic-authentication
def basic_auth(username, password):
    token = b64encode(f"{username}:{password}".encode('utf-8')).decode('utf-8')
    return f'Basic {token}'


def get_noga_data():
    # Get nomenclature data from i14y.admin.ch
    dfs_nomenclature_noga = {}
    for i in range(1, 6):
        dfs_nomenclature_noga[i] = get_noga_nomenclature(i)
    df_nc_noga_flat = flatten_nomenclature(dfs_nomenclature_noga)
    path_nomenclature_flat = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'nomenclature_noga_flat.csv')
    df_nc_noga_flat.to_csv(path_nomenclature_flat, index=False)
    df_nc_noga_flat['url_kubb'] = f'https://www.kubb-tool.bfs.admin.ch/de/code/{df_nc_noga_flat["noga_code"]}'
    if ct.has_changed(path_nomenclature_flat):
        logging.info(f'Exporting noga_nomenclature.csv to FTP server')
        common.upload_ftp(path_nomenclature_flat, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'zefix_handelsregister')
        ct.update_hash_file(path_nomenclature_flat)
    return get_burweb_data(df_nc_noga_flat)


def get_noga_nomenclature(level):
    # API-Query for NOGA nomenclature can be created here:
    # https://www.i14y.admin.ch/de/catalog/datasets/HCL_NOGA/api
    url_noga = f'https://www.i14y.admin.ch/api/Nomenclatures/HCL_NOGA/levelexport/CSV?level={level}'
    r = common.requests_get(url_noga)
    path_nomenclature = os.path.join(pathlib.Path(__file__).parents[0], 'data', f'nomenclature_noga_lv{level}.csv')
    with open(path_nomenclature, 'wb') as f:
        f.write(r.content)
    df_nomenclature_noga = pd.read_csv(path_nomenclature, dtype=str)
    return df_nomenclature_noga


def flatten_nomenclature(dfs_nomenclature_noga):
    df_noga_all_nc = dfs_nomenclature_noga[5]
    names = ['_abteilung', '_gruppe', '_klasse', '']
    # Iterate from 4 to 1
    for i in range(4, 0, -1):
        # Rename before merge of next level
        df_noga_all_nc = df_noga_all_nc.rename(columns={'Code': f'noga{names[i - 1]}_code'})
        df_noga_all_nc = df_noga_all_nc.rename(columns={'Parent': 'Code'})
        for lng in ['de', 'fr', 'it', 'en']:
            df_noga_all_nc = df_noga_all_nc.rename(columns={f'Name_{lng}': f'noga{names[i - 1]}_{lng}'})
        # Merge with next level
        df_noga_all_nc = pd.merge(df_noga_all_nc, dfs_nomenclature_noga[i], on='Code')

    df_noga_all_nc = df_noga_all_nc.rename(columns={'Code': 'noga_abschnitt_code'})
    df_noga_all_nc = df_noga_all_nc.drop(columns=['Parent'])
    for lng in ['de', 'fr', 'it', 'en']:
        df_noga_all_nc = df_noga_all_nc.rename(columns={f'Name_{lng}': f'noga_abschnitt_{lng}'})

    return df_noga_all_nc


def get_burweb_data(df_nc_noga):
    url = 'https://www.burweb2.admin.ch/BurWeb.Services.External/V1_8/ExtractV1X8/Full'
    headers = {'Authorization': basic_auth(credentials.user_burweb, credentials.pass_burweb)}
    # Stream the download
    with common.requests_get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        path_xml = os.path.join(pathlib.Path(__file__).parents[0], 'data', 'burweb_full_extract.xml')

        # Write chunks to file
        with open(path_xml, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    tree = ET.parse(path_xml)
    enterprise_units = tree.findall('.//enterpriseUnit')

    # Parsing the XML and storing the data
    data = []
    for unit in enterprise_units:
        legal_id = unit.findtext('legalId')
        noga2008 = unit.findtext('.//noga2008')
        data.append([legal_id, noga2008])
    df_burweb = pd.DataFrame(data, columns=['company_uid', 'noga_code'])
    # Merge with noga data
    df_burweb = pd.merge(df_burweb, df_nc_noga, on='noga_code', how='left')
    return df_burweb
'''