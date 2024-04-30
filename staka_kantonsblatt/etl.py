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
    df = remove_entries(df)
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


def remove_entries(df):
    # Remove the data of the registration offices and the on behalf of that contain names of persons
    entries_to_remove_file = os.path.join(credentials.data_path, 'kantonsblatt_entries_to_remove_from_OGD.xlsx')
    new_values = {}
    df_sheets = {}
    logging.info(f'Reading Excel file {entries_to_remove_file}...')
    for sheet in ['registrationOfficeDisplayName', 'onBehalfOf']:
        df_lookup = pd.read_excel(entries_to_remove_file, sheet_name=sheet, dtype={sheet: str, 'remove': bool})
        df = df.merge(df_lookup, how='left', on=sheet)
        df.loc[df[sheet].notna() & df['remove'], sheet] = ''
        new_values[sheet] = df.loc[df[sheet].notna() & df['remove'].isna(), sheet].unique()
        df.loc[df[sheet].notna() & df['remove'].isna(), sheet] = ''
        if sheet == 'registrationOfficeDisplayName':
            columns_to_remove = ['registrationOfficeStreet', 'registrationOfficeStreetNumber',
                                 'registrationOfficePostOfficeBoxNumber']
            df.loc[df['remove'] | df['remove'].isna(), columns_to_remove] = ''
        logging.info(f'New values for {sheet}: {new_values[sheet]}')
        df_new_values = pd.DataFrame({sheet: new_values[sheet], 'remove': [True] * len(new_values[sheet])})
        df_sheets[sheet] = pd.concat([df_lookup, df_new_values])
        df = df.drop(columns='remove')

    # Send an e-mail with new values
    if 'registrationOfficeDisplayName' in new_values or 'onBehalfOf' in new_values:
        logging.info(f'Writing Excel file {entries_to_remove_file}...')
        with pd.ExcelWriter(entries_to_remove_file) as writer:
            for sheet in df_sheets:
                df_sheets[sheet].to_excel(writer, sheet_name=sheet, index=False)
        logging.info('Sending e-mail...')
        text = "The dataset of the Kantonsblatt (https://data.bs.ch/explore/dataset/100352) "
        text += "has two columns, 'registrationOfficeDisplayName' and 'onBehalfOf' "
        text += "which can contain names of persons. If there is a new value, it is removed by default.\n\n"

        text += "Please check if the new values are names of persons and if they should be removed.\n"
        text += "If not, please change the boolean flag in the Excel file "
        text += "so they are added again in the next run of the job.\n\n"

        text += "The new values are:\n"
        if 'registrationOfficeDisplayName' in new_values:
            text += "\nRegistration offices (found in the sheet 'registrationOfficeDisplayName'):\n"
            for value in new_values['registrationOfficeDisplayName']:
                text += f" - {value}\n"
        if 'onBehalfOf' in new_values:
            text += f"\nOn behalf of (found in the sheet 'onBehalfOf'):\n"
            for value in new_values['onBehalfOf']:
                text += f" - {value}\n"
        text += f"\nThe Excel-File is located here:\n {credentials.excel_path_for_mail}\n"
        text += "\nKind regards, \nYour automated Open Data Basel-Stadt Python Job"
        msg = common.email_message(subject="Data removed from Kantonsblatt (100352). Please check if person data.",
                                   text=text, img=None, attachment=None)
        common.send_email(msg)
    return df


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
