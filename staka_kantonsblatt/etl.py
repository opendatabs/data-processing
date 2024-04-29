import os
import io
import logging
import pandas as pd
import datetime

import common
from staka_kantonsblatt import credentials


# References:
# https://www.amtsblattportal.ch/docs/api/


def main():
    df = iterate_over_years()
    # Get names for the rubric codes
    df_rubric, df_subRubric = get_rubric_from_api()
    df = df.merge(df_rubric, how='left', on='rubric')
    df = df.merge(df_subRubric, how='left', on='subRubric')
    # Get names for the tenant codes
    tenant_code_to_name = get_tenants_from_api()
    df['primaryTenantName'] = df['primaryTenantCode'].map(tenant_code_to_name)
    df['secondaryTenantsTenantName'] = df.loc[
        df['secondaryTenantsTenantCode'].notna(), 'secondaryTenantsTenantCode'].str.split(',').apply(
        lambda x: ','.join([tenant_code_to_name.get(y) for y in x]))
    df = anonymize_data(df)
    columns_of_interest = ['id', 'rubric', 'rubric_de', 'subRubric', 'subRubric_de', 'language', 'registrationOfficeId',
                           'registrationOfficeDisplayName', 'registrationOfficeStreet',
                           'registrationOfficeStreetNumber', 'registrationOfficeSwissZipCode', 'registrationOfficeTown',
                           'registrationOfficeContainsPostOfficeBox', 'registrationOfficePostOfficeBoxNumber',
                           'registrationOfficePostOfficeBoxSwissZipCode', 'registrationOfficePostOfficeBoxTown',
                           'publicationNumber', 'publicationState', 'publicationDate', 'expirationDate',
                           'primaryTenantCode', 'primaryTenantName', 'onBehalfOf', 'legalRemedy', 'cantons',
                           'secondaryTenantsTenantCode', 'secondaryTenantsTenantName',
                           'secondaryTenantsPublicationDate', 'repeatedPublicationsPublicationNumber',
                           'repeatedPublicationsPublicationDate', 'url_kantonsblatt', 'url_pdf', 'url_xml']
    df = df[columns_of_interest]
    path_export = os.path.join(credentials.data_path, 'export', '100352_kantonsblatt.csv')
    df.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export, 'staka/kantonsblatt', '100352')


def iterate_over_newest_pages(pages=10):
    base_url = 'https://kantonsblatt.ch/api/v1/publications/csv?publicationStates=PUBLISHED&cantons=BS'
    url = f'{base_url}&pageRequest.page=0'
    for page in range(pages):
        logging.info(f'Getting data from {url}...')
        r = common.requests_get(url)
        r.raise_for_status()
        df_curr_page = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')
        df_curr_page = add_columns(df_curr_page)
        common.ods_realtime_push_df(df_curr_page, credentials.push_url)
        url = f'{base_url}&pageRequest.page={page + 1}'


def iterate_over_years():
    start_year = 2019
    df = pd.DataFrame()
    for year in range(start_year, datetime.datetime.now().year + 1):
        for month in range(1, 13):
            if year == datetime.datetime.now().year and month > datetime.datetime.now().month:
                break
            logging.info(f'Getting data for {year}-{month}...')
            df_month = iterate_over_pages(year, month)
            df = pd.concat([df, df_month])
    return df


def iterate_over_pages(year, month):
    base_url = f'https://kantonsblatt.ch/api/v1/publications/csv?publicationStates=PUBLISHED&cantons=BS'
    start_date = f'&publicationDate.start={year}-{month}-01'
    end_date = f'&publicationDate.end={year}-{month + 1}-01' if month < 12 else f'&publicationDate.end={year + 1}-01-01'
    url = f'{base_url}{start_date}{end_date}'
    page = 0
    next_page = f'{url}&pageRequest.page={page}'
    df = pd.DataFrame()
    while True:
        logging.info(f'Getting data from {next_page}...')
        r = common.requests_get(next_page)
        r.raise_for_status()
        df_curr_page = pd.read_csv(io.StringIO(r.content.decode('utf-8')), sep=';')
        if df_curr_page.empty:
            break
        df_curr_page = add_columns(df_curr_page)
        df = pd.concat([df, df_curr_page])
        page = page + 1
        next_page = f'{url}&pageRequest.page={page}'
    return df


def add_columns(df):
    df['url_kantonsblatt'] = df['id'].apply(lambda x: f'https://www.kantonsblatt.ch/#!/search/publications/detail/{x}')
    df['url_pdf'] = df['id'].apply(lambda x: f'https://www.kantonsblatt.ch/api/v1/publications/{x}/pdf')
    df['url_xml'] = df['id'].apply(lambda x: f'https://www.kantonsblatt.ch/api/v1/publications/{x}/xml')
    return df


def get_rubric_from_api():
    url = 'https://www.kantonsblatt.ch/api/v1/rubrics'
    r = common.requests_get(url)
    r.raise_for_status()
    df = pd.read_json(io.StringIO(r.content.decode('utf-8')))
    df = df.rename(columns={'code': 'rubric'})
    df = pd.concat([df[['rubric', 'subRubrics']], pd.json_normalize(df['name'])], axis=1)
    df_rubric = df.rename(columns={'en': 'rubric_en', 'de': 'rubric_de', 'fr': 'rubric_fr',
                                   'it': 'rubric_it'})[['rubric', 'rubric_en', 'rubric_de', 'rubric_fr', 'rubric_it']]
    df = df.explode('subRubrics').reset_index(drop=True)
    df = pd.json_normalize(df['subRubrics'])[['code', 'name.en', 'name.de', 'name.fr', 'name.it']]
    df_subRubric = df.rename(columns={'code': 'subRubric', 'name.en': 'subRubric_en', 'name.de': 'subRubric_de',
                                      'name.fr': 'subRubric_fr', 'name.it': 'subRubric_it'})
    return df_rubric, df_subRubric


def get_tenants_from_api():
    url = 'https://www.kantonsblatt.ch/api/v1/tenants'
    r = common.requests_get(url)
    r.raise_for_status()
    tenants = r.json()
    return {tenant['id']: tenant['title']['de'] for tenant in tenants}


def anonymize_data(df):
    # Anonymize the data of the registration offices and the on behalf of
    df_reg_office = pd.read_excel(os.path.join(credentials.data_path, 'lookup_table_registration_office.xlsx'),
                                  dtype={'registrationOfficeDisplayName': str, 'anonymize': bool})
    df = df.merge(df_reg_office, how='left', on='registrationOfficeDisplayName')
    df.loc[df['registrationOfficeDisplayName'].notna() & df['anonymize'], 'registrationOfficeDisplayName'] = ''
    df.loc[df['registrationOfficeDisplayName'].notna() & df['anonymize'].isna(), 'registrationOfficeDisplayName'] = ''
    new_values_reg_office = df.loc[
        df['registrationOfficeDisplayName'].notna() & df['anonymize'].isna(), 'registrationOfficeDisplayName'].unique()
    df = df.drop(columns='anonymize')
    df_reg_office = pd.concat([df_reg_office, pd.DataFrame({'registrationOfficeDisplayName': new_values_reg_office,
                                                            'anonymize': True})])
    df_reg_office.to_excel(os.path.join(credentials.data_path, 'lookup_table_registration_office.xlsx'), index=False)

    df_on_behalf = pd.read_excel(os.path.join(credentials.data_path, 'lookup_table_on_behalf.xlsx'),
                                 dtype={'onBehalfOf': str, 'anonymize': bool})
    df = df.merge(df_on_behalf, how='left', on='onBehalfOf')
    df.loc[df['onBehalfOf'].notna() & df['anonymize'], 'onBehalfOf'] = ''
    df.loc[df['onBehalfOf'].notna() & df['anonymize'].isna(), 'onBehalfOf'] = ''
    new_values_on_behalf = df.loc[df['onBehalfOf'].notna() & df['anonymize'].isna(), 'onBehalfOf'].unique()
    df = df.drop(columns='anonymize')
    df_on_behalf = pd.concat([df_on_behalf, pd.DataFrame({'onBehalfOf': new_values_on_behalf, 'anonymize': True})])
    df_on_behalf.to_excel(os.path.join(credentials.data_path, 'lookup_table_on_behalf.xlsx'), index=False)

    # Send an e-mail with new values
    if new_values_on_behalf.size > 0 or new_values_reg_office.size > 0:
        text = "New values have been added to the lookup tables for the anonymization (and set to True by default) " \
               "of the registration offices and the on behalf of. The new values are:\n\n"
        if new_values_reg_office.size > 0:
            text += "Registration offices:\n"
            for value in new_values_reg_office:
                text += f" - {value}\n"
        if new_values_on_behalf.size > 0:
            text += f"On behalf of:\n"
            for value in new_values_on_behalf:
                text += f" - {value}\n"
        text += "\bKind regards, \nYour automated Open Data Basel-Stadt Python Job"
        msg = common.email_message(subject="New values for anonymization", text=text, img=None, attachment=None)
        common.send_email(msg)
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
