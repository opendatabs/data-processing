import os
import pandas as pd
import logging
import pathlib
from datetime import datetime

from parlamentsdienst_grosserrat import credentials
import common
import common.change_tracking as ct
import ods_publish.etl_id as odsp


def main():
    """
    This python-file reads various CSV files containing different types of data, processes them, and creates
    corresponding CSV files for each type of data.

    It performs the following steps:
    1. Read CSV files containing data about addresses, memberships, committees, interests, businesses,
       associates, assignments, documents, processes, and meetings.
    2. Process and modify the data
    3. Create CSV files for data.bs.ch
    """
    logging.info(f'Reading Personen.csv...')
    df_adr = common.pandas_read_csv(credentials.path_adr, encoding='utf-8', dtype=str)
    logging.info(f'Reading Mitgliedschaften.csv...')
    df_mit = common.pandas_read_csv(credentials.path_mit, encoding='utf-8', dtype=str)
    logging.info(f'Reading Gremien.csv...')
    df_gre = common.pandas_read_csv(credentials.path_gre, encoding='utf-8', dtype=str)
    logging.info(f'Reading Interessensbindungen.csv...')
    df_intr = common.pandas_read_csv(credentials.path_intr, encoding='utf-8', dtype=str)

    logging.info(f'Reading Geschäfte.csv...')
    df_ges = common.pandas_read_csv(credentials.path_ges, encoding='utf-8', dtype=str)
    # Replace identifiers to match with values in the committee list (gremium.csv)
    df_ges['gr_urheber'] = df_ges['gr_urheber'].replace({'1934': '3', '4276': '2910', '4278': '3164',
                                                         '4279': '3196', '4280': '3331'})

    logging.info(f'Reading Konsorten.csv...')
    df_kon = common.pandas_read_csv(credentials.path_kon, encoding='utf-8', dtype=str)
    df_kon['uni_nr_adr'] = df_kon['uni_nr_adr'].replace({'1934': '3', '4276': '2910', '4278': '3164',
                                                         '4279': '3196', '4280': '3331'})

    logging.info(f'Reading Zuweisungen.csv...')
    df_zuw = common.pandas_read_csv(credentials.path_zuw, encoding='utf-8', dtype=str)
    # Temporarily replace 'Parlamentsdienst' (1934) with 'Grosser Rat' (3)
    df_zuw = df_zuw.replace({'uni_nr_von': '1934', 'uni_nr_an': '1934'}, '3')

    # Replace other committee identifiers for consistency with committee list (gremium.csv)
    df_zuw = df_zuw.replace({'uni_nr_von': '4276', 'uni_nr_an': '4276'}, '2910')  # IPK-FHN
    df_zuw = df_zuw.replace({'uni_nr_von': '4278', 'uni_nr_an': '4276'}, '3164')  # IGPK-UK
    df_zuw = df_zuw.replace({'uni_nr_von': '4279', 'uni_nr_an': '4279'}, '3196')  # IGPK-Un
    df_zuw = df_zuw.replace({'uni_nr_von': '4280', 'uni_nr_an': '4280'}, '3331')  # IGPK-Ha

    logging.info(f'Reading Dokumente.csv...')
    df_dok = common.pandas_read_csv(credentials.path_dok, encoding='utf-8', dtype=str)
    logging.info(f'Reading Vorgänge.csv...')
    df_vor = common.pandas_read_csv(credentials.path_vor, encoding='utf-8', dtype=str)
    logging.info(f'Reading Sitzungen.csv...')
    df_siz = common.pandas_read_csv(credentials.path_siz, encoding='utf-8', dtype=str)

    # Perform data processing and CSV file creation functions
    create_mitglieder_csv(df_adr, df_mit)
    create_mitgliedschaften_csv(df_adr, df_mit, df_gre)
    create_interessensbindungen_csv(df_adr, df_intr)
    create_gremien_csv(df_gre, df_mit)
    create_geschaefte_csv(df_adr, df_ges, df_kon, df_gre)
    create_zuweisungen_csv(df_gre, df_ges, df_zuw)
    create_dokumente_csv(df_adr, df_ges, df_dok)
    create_vorgaenge_csv(df_ges, df_vor, df_siz)


def create_mitglieder_csv(df_adr, df_mit):
    """
    Create a CSV file containing information about members of the Grosser Rat (Parliament).

    Args:
        df_adr (pd.DataFrame): DataFrame containing person information.
        df_mit (pd.DataFrame): DataFrame containing membership information.

    Returns:
        None
    """
    # Select members of Grosser Rat without specific functions
    # since functions are always recorded as part of an entire membership
    # Not ignoring it would lead to duplicated memberships
    df_gr = df_mit[(df_mit['uni_nr_gre'] == '3') & (df_mit['funktion'].isna())]
    df = pd.merge(df_adr, df_gr, left_on='uni_nr', right_on='uni_nr_adr')

    # Rename columns for clarity
    df = df.rename(columns={'beginn': 'gr_beginn', 'ende': 'gr_ende'})

    # Check if the membership is currently active in Grosser Rat
    df['ist_aktuell_grossrat'] = df['gr_ende'] == credentials.unix_ts_max

    df['url'] = credentials.path_personen + df['uni_nr']

    # Select relevant columns for publication
    cols_of_interest = [
        'ist_aktuell_grossrat', 'anrede', 'titel', 'name', 'vorname', 'gebdatum', 'gr_sitzplatz',
        'gr_wahlkreis', 'partei', 'partei_kname', 'gr_beginn', 'gr_ende', 'url', 'uni_nr',
        'strasse', 'plz', 'ort', 'gr_beruf', 'gr_arbeitgeber', 'telefong', 'telefonm', 'telefonp',
        'emailg', 'emailp', 'homepage'
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ['gr_beginn', 'gr_ende'])

    logging.info(f'Creating dataset "Personen im Grossen Rat"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_mitglieder.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'mitglieder', '100307')


def create_mitgliedschaften_csv(df_adr, df_mit, df_gre):
    """
        Creates a CSV file containing membership information in committees.

        Args:
        df_adr (pd.DataFrame): DataFrame containing person information.
        df_mit (pd.DataFrame): DataFrame containing membership information.
        df_gre (pd.DataFrame): DataFrame containing committee information.

    Returns:
        None
    """
    df = pd.merge(df_gre, df_mit, left_on='uni_nr', right_on='uni_nr_gre')
    df = pd.merge(df, df_adr, left_on='uni_nr_adr', right_on='uni_nr')

    # Rename columns for clarity
    df = df.rename(columns={'name_x': 'name_gre', 'name_y': 'name_adr',
                            'beginn': 'beginn_mit', 'ende': 'ende_mit',
                            'kurzname': 'kurzname_gre', 'vorname': 'vorname_adr',
                            'funktion': 'funktion_adr'})

    df['url_adr'] = credentials.path_personen + df['uni_nr_adr']
    # URL for committee page (currently removed)
    # df['url_gre'] = credentials.path_gremien + df['uni_nr_gre']

    # Select relevant columns for publication
    cols_of_interest = [
        'kurzname_gre', 'name_gre', 'gremientyp', 'uni_nr_gre', 'beginn_mit', 'ende_mit',
        'funktion_adr', 'anrede', 'name_adr', 'vorname_adr', 'partei_kname', 'url_adr', 'uni_nr_adr'
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ['beginn_mit', 'ende_mit'])

    logging.info(f'Creating dataset "Mitgliedschaften in Gremien"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_mitgliedschaften.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'mitgliedschaften', '100308')


def create_interessensbindungen_csv(df_adr, df_intr):
    """
    Creates a CSV file containing information about interest bindings.

    Args:
        df_adr (pd.DataFrame): DataFrame containing person information.
        df_intr (pd.DataFrame): DataFrame containing stakeholder information.

    Returns:
        None
    """
    df = pd.merge(df_intr, df_adr, left_on='idnr_adr', right_on='idnr')

    # Splitting 'text' column to separate 'intr-bind' and 'funktion'
    df['pos_of_('] = df['text'].str.rfind('(')
    df['intr-bind'] = df.apply(lambda x: x['text'][:x['pos_of_('] - 1], axis=1)
    df['funktion'] = df.apply(lambda x: x['text'][x['pos_of_('] + 1:-1], axis=1)
    # URL erstellen
    df['url_adr'] = credentials.path_personen + df['uni_nr']

    # Select relevant columns for publication
    cols_of_interest = [
        'rubrik', 'intr-bind', 'funktion', 'text',
        'anrede', 'name', 'vorname', 'partei_kname', 'url_adr', 'uni_nr'
    ]
    df = df[cols_of_interest]

    logging.info(f'Creating dataset "Mitgliedschaften in Interessensbindungen"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_interessensbindungen.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'interessensbindungen', '100309')


def create_gremien_csv(df_gre, df_mit):
    """
    Creates a CSV file containing information about committees in the "Grosser Rat".

    Args:
        df_gre (pd.DataFrame): DataFrame containing committee information.
        df_mit (pd.DataFrame): DataFrame containing membership information.

    Returns:
        None
    """

    # To check which committees are currently active, we look at committees with current memberships
    # (with a 3-month buffer due to commissions sometimes lacking members for a while after a legislative period)
    unix_ts = (datetime.now() - datetime(1970, 4, 1)).total_seconds()
    df_mit['ist_aktuelles_gremium'] = df_mit['ende'].astype(int) > unix_ts

    df_mit = df_mit.groupby('uni_nr_gre').any('ist_aktuelles_gremium')

    df = pd.merge(df_gre, df_mit, left_on='uni_nr', right_on='uni_nr_gre')

    # URL for the committee's page (currently removed)
    # TODO: Add using Sitemap XML for current committees.
    # df['url_gre'] = credentials.path_gremium + df['uni_nr']

    # Select relevant columns for publication
    cols_of_interest = [
        'ist_aktuelles_gremium', 'kurzname', 'name', 'gremientyp', 'uni_nr'
    ]
    df = df[cols_of_interest]

    logging.info(f'Creating Datensatz "Gremien im Grossen Rat"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_gremien.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'gremien', '100310')


def create_geschaefte_csv(df_adr, df_ges, df_kon, df_gre):
    """
    Creates a CSV file containing information about matters (Geschäfte) in the parliament.

    Args:
        df_adr (pd.DataFrame): DataFrame containing person information.
        df_ges (pd.DataFrame): DataFrame containing matters information.
        df_kon (pd.DataFrame): DataFrame containing consortium information.
        df_gre (pd.DataFrame): DataFrame containing committee information.

    Returns:
        None
    """

    df = pd.merge(df_ges, df_adr, how='left', left_on='gr_urheber', right_on='uni_nr', suffixes=('_ges', '_adr'))
    # Konsorten hinzufügen
    df = pd.merge(df, df_kon, how='left', left_on='laufnr', right_on='ges_laufnr')
    df = pd.merge(df, df_adr, how='left', left_on='uni_nr_adr', right_on='uni_nr', suffixes=('_urheber', '_miturheber'))

    # Rename columns for clarity
    df = df.rename(columns={'beginn': 'beginn_ges', 'ende': 'ende_ges',
                            'laufnr': 'laufnr_ges', 'status': 'status_ges',
                            'signatur': 'signatur_ges', 'departement': 'departement_ges',
                            'gr_urheber': 'nr_urheber', 'uni_nr_adr': 'nr_miturheber'})

    df['url_ges'] = credentials.path_geschaeft + df['signatur_ges']
    # Replacing status codes with their meanings
    df['status_ges'] = df['status_ges'].replace({'A': 'Abgeschlossen', 'B': 'In Bearbeitung'})

    df['url_urheber'] = credentials.path_personen + df['nr_urheber'][df['nr_urheber'].notna()]
    df['url_urheber_ratsmitgl'] = ('https://data.bs.ch/explore/dataset/100307/?refine.uni_nr=' +
                                   df['nr_urheber'][df['nr_urheber'].notna()])
    # If the "Urheber" is a committee (gremium), no link should be created
    df.loc[df['vorname_urheber'].isna(), 'url_urheber'] = float('nan')
    df.loc[df['vorname_urheber'].isna(), 'url_urheber_ratsmitgl'] = float('nan')
    # Fields for names of person can be used for the committee as follows
    df.loc[df['vorname_urheber'].isna(), 'gremientyp_urheber'] = 'Kommission'
    df.loc[df['vorname_urheber'].isna(), 'name_urheber'] = df['nr_urheber'].map(
        df_gre.set_index('uni_nr')['name'])
    df.loc[df['vorname_urheber'].isna(), 'vorname_urheber'] = df['nr_urheber'].map(
        df_gre.set_index('uni_nr')['kurzname'])
    # If 'vorname_urheber' is still empty, it's "Regierungsrat"
    df.loc[df['vorname_urheber'].isna(), 'gremientyp_urheber'] = 'Regierungsrat'

    # Similar approach for Miturheber
    df['url_miturheber'] = credentials.path_personen + df['nr_miturheber'][df['nr_miturheber'].notna()]
    df['url_miturheber_ratsmitgl'] = ('https://data.bs.ch/explore/dataset/100307/?refine.uni_nr=' +
                                   df['nr_miturheber'][df['nr_miturheber'].notna()])
    df.loc[df['vorname_miturheber'].isna(), 'url_miturheber'] = float('nan')
    df.loc[df['vorname_miturheber'].isna(), 'url_miturheber_ratsmitgl'] = float('nan')
    df.loc[df['vorname_miturheber'].isna(), 'gremientyp_miturheber'] = 'Kommission'
    df.loc[df['vorname_miturheber'].isna(), 'name_miturheber'] = df['nr_miturheber'].map(
        df_gre.set_index('uni_nr')['name'])
    df.loc[df['vorname_miturheber'].isna(), 'vorname_miturheber'] = df['nr_miturheber'].map(
        df_gre.set_index('uni_nr')['kurzname'])
    # If 'vorname_miturheber' is still empty, there is none
    df.loc[df['vorname_miturheber'].isna(), 'gremientyp_miturheber'] = float('nan')
    # but if 'vorname_miturheber' is empty and there is a 'nr_miturheber' it should be 'Regierungsrat'
    df.loc[(df['vorname_miturheber'].isna()) & (df['nr_miturheber'].notna()), 'gremientyp_miturheber'] = 'Regierungsrat'

    # Select relevant columns for publication
    cols_of_interest = [
        'beginn_ges', 'ende_ges', 'laufnr_ges', 'signatur_ges', 'status_ges',
        'titel_ges', 'departement_ges', 'ga_rr_gr', 'url_ges',
        'anrede_urheber', 'gremientyp_urheber','name_urheber', 'vorname_urheber',
        'partei_kname_urheber', 'url_urheber', 'nr_urheber', 'url_urheber_ratsmitgl',
        'anrede_miturheber', 'gremientyp_miturheber', 'name_miturheber', 'vorname_miturheber',
        'partei_kname_miturheber', 'url_miturheber', 'nr_miturheber', 'url_miturheber_ratsmitgl'
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ['beginn_ges', 'ende_ges'])

    logging.info(f'Creating dataset "Geschäfte im Grossen Rat"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_geschaefte.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'geschaefte', '100311')


def create_zuweisungen_csv(df_gre, df_ges, df_zuw):
    """
    Creates a CSV file containing information about assignments of matters to committees.

    Args:
        df_gre (pd.DataFrame): DataFrame containing committee information.
        df_ges (pd.DataFrame): DataFrame containing matters information.
        df_zuw (pd.DataFrame): DataFrame containing assignment information.

    Returns:
        None
    """

    """ Temporary alternative solution for now
    # The following entries need to be added manually
    df_to_add = pd.DataFrame([[4276, 'GR-IPK-FHN', 'Delegation IPK Fachhochschule Nordwestschweiz'],
                              [4278, 'GR-IGPK-UK', 'Delegation IGPK Uni-Kinderspital beider Basel'],
                              [4279, 'GR-IGPK-Un', 'Delegation IGPK Universität Basel'],
                              [4280, 'GR-IGPK-Ha', 'Delegation IGPK Schweizer Rheinhäfen']],
                             columns=['uni_nr', 'kurzname', 'name'])
    df_gre = pd.concat([df_gre, df_to_add])
    """

    # All entries not present in gremium.csv are still inserted and treated as "Regierungsrat"
    df = pd.merge(df_gre, df_zuw, how='right', left_on='uni_nr', right_on='uni_nr_an')
    # Removing the column due to the following merging to avoid duplicate columns
    df = df.drop(['uni_nr'], axis=1)
    df = pd.merge(df, df_ges, left_on='ges_laufnr', right_on='laufnr', suffixes=('_zuw', '_ges'))
    df = pd.merge(df, df_gre, how='left', left_on='uni_nr_von', right_on='uni_nr', suffixes=('_an', '_von'))

    # Rename columns for clarity
    df = df.rename(columns={'beginn': 'beginn_ges', 'ende': 'ende_ges',
                            'laufnr': 'laufnr_ges', 'status': 'status_ges',
                            'signatur': 'signatur_ges', 'departement': 'departement_ges'})

    # Temporarily replacing remaining committees not in committee list with "Regierungsrat" (without number)
    values = {'kurzname_an': 'RR', 'kurzname_von': 'RR', 'name_an': 'Regierungsrat', 'name_von': 'Regierungsrat'}
    df = df.fillna(value=values)
    df.loc[df['name_an'] == 'Regierungsrat', 'uni_nr_an'] = float('nan')
    df.loc[df['name_von'] == 'Regierungsrat', 'uni_nr_von'] = float('nan')
    # Replacing status codes with their meanings# Stati überall mit dessen Bedeutung ersetzen
    df['status_zuw'] = df['status_zuw'].replace({'A': 'Abgeschlossen', 'B': 'In Bearbeitung',
                                                 'X': 'Abgebrochen', 'F': 'Fertig'})
    df['status_ges'] = df['status_ges'].replace({'A': 'Abgeschlossen', 'B': 'In Bearbeitung'})

    df['url_ges'] = credentials.path_geschaeft + df['signatur_ges']
    ''' URL for committee's page (currently removed)
    df['url_gre_an'] = credentials.path_gremien + df['uni_nr_an']
    df['url_gre_von'] = credentials.path_gremien + df['uni_nr_von']
    '''

    # Select relevant columns for publication
    cols_of_interest = [
        'kurzname_an', 'name_an', 'uni_nr_an', 'erledigt',
        'status_zuw', 'termin', 'titel_zuw', 'bem',
        'beginn_ges', 'ende_ges', 'laufnr_ges', 'signatur_ges', 'status_ges',
        'titel_ges', 'ga_rr_gr', 'departement_ges', 'url_ges',
        'kurzname_von', 'name_von', 'uni_nr_von'
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ['erledigt', 'termin', 'beginn_ges', 'ende_ges'])

    logging.info(f'Creating Datensatz "Zuweisungen Geschäfte"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_zuweisungen.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'zuweisungen', '100312')


# TODO: Nochmals upzudaten wenn OGD-Export geändert wird
def create_dokumente_csv(df_adr, df_ges, df_dok):
    """
    Creates a CSV file containing information about documents related to matters.

    Args:
        df_adr (pd.DataFrame): DataFrame containing person information.
        df_ges (pd.DataFrame): DataFrame containing matters information.
        df_dok (pd.DataFrame): DataFrame containing document information.

    Returns:
        None
    """
    df = pd.merge(df_dok, df_ges, left_on='ges_laufnr', right_on='laufnr', suffixes=('_dok', '_ges'))
    df = pd.merge(df, df_adr, how='left', left_on='gr_urheber', right_on='uni_nr')

    # Rename columns for clarity
    df = df.rename(columns={'beginn': 'beginn_ges', 'ende': 'ende_ges',
                            'laufnr': 'laufnr_ges', 'status': 'status_ges',
                            'signatur': 'signatur_ges', 'departement': 'departement_ges'})

    df['url_dok'] = df['url']
    # Wait for Permalink
    # df['url_dok'] = credentials.path_dokument + df['dok_nr']
    df['url_ges'] = credentials.path_geschaeft + df['signatur_ges']

    # Replacing status codes with their meanings
    df['status_ges'] = df['status_ges'].replace({'A': 'Abgeschlossen', 'B': 'In Bearbeitung'})

    # Select relevant columns for publication
    cols_of_interest = [
        'dokudatum', 'dok_nr', 'titel_dok', 'url_dok',
        'beginn_ges', 'ende_ges', 'laufnr_ges', 'signatur_ges', 'status_ges',
        'titel_ges', 'ga_rr_gr', 'departement_ges', 'url_ges'
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ['dokudatum', 'beginn_ges', 'ende_ges'])

    # Temporarily
    df = df.rename(columns={'dok_nr': 'dok_laufnr'})

    logging.info(f'Creating Datensatz "Dokumente Geschäfte"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_dokumente.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'dokumente', '100313')


def create_vorgaenge_csv(df_ges, df_vor, df_siz):
    """
    Creates a CSV file containing information about processes associated with matters.

    Args:
        df_ges (pd.DataFrame): DataFrame containing matters information.
        df_vor (pd.DataFrame): DataFrame containing process information.
        df_siz (pd.DataFrame): DataFrame containing session information.

    Returns:
        None
    """
    df = pd.merge(df_vor, df_ges, left_on='ges_laufnr', right_on='laufnr')
    df = pd.merge(df, df_siz, on='siz_nr')

    # Rename columns for clarity
    df = df.rename(columns={'beginn': 'beginn_ges', 'ende': 'ende_ges',
                            'laufnr': 'laufnr_ges', 'status': 'status_ges',
                            'signatur': 'signatur_ges', 'departement': 'departement_ges',
                            'titel': 'titel_ges', 'datum': 'siz_datum'})

    df['url_ges'] = credentials.path_geschaeft + df['signatur_ges']

    # Replacing status codes with their meanings
    df['status_ges'] = df['status_ges'].replace({'A': 'Abgeschlossen', 'B': 'In Bearbeitung'})

    # Select relevant columns for publication
    cols_of_interest = [
        'beschlnr', 'nummer', 'Vermerk', 'siz_nr', 'siz_datum',
        'beginn_ges', 'ende_ges', 'laufnr_ges', 'signatur_ges', 'status_ges',
        'titel_ges', 'ga_rr_gr', 'departement_ges', 'url_ges'
    ]
    df = df[cols_of_interest]

    # Convert Unix Timestamp to Datetime for date columns
    df = unix_to_datetime(df, ['siz_datum', 'beginn_ges', 'ende_ges'])

    logging.info(f'Creating Datensatz "Vorgänge Geschäfte"...')
    path_export = os.path.join(pathlib.Path(__file__).parents[1],
                               'parlamentsdienst_grosserrat/data/export/grosser_rat_vorgaenge.csv')
    df.to_csv(path_export, index=False)
    update_ftp_and_odsp(path_export, 'vorgaenge', '100314')


def unix_to_datetime(df, column_names):
    """
    Converts Unix timestamps in specified columns of a DataFrame to datetime format.

    Args:
        df (pd.DataFrame): DataFrame to be processed.
        column_names (list): List of column names containing Unix timestamps.

    Returns:
        pd.DataFrame: DataFrame with converted datetime values.
    """
    # Replace '0' values with NaN to handle missing timestamps
    df[column_names] = df[column_names].replace('0', float('NaN'))

    # Loop through each specified column and convert Unix timestamps to formatted datetime strings
    for column_name in column_names:
        df[column_name] = pd.to_datetime(df[column_name].astype(float), unit='s', errors='coerce').dt.strftime('%Y-%m-%d')
    return df


def update_ftp_and_odsp(path_export, dataset_name, dataset_id):
    """
    Updates a dataset by uploading it to an FTP server and publishing it into data.bs.ch.

    This function performs the following steps:
    1. Checks if the content of the dataset at the specified path has changed.
    2. If changes are detected, uploads the dataset to an FTP server using provided credentials.
    3. Publishes the dataset into data.bs.ch using the provided dataset ID.
    4. Updates the hash file to reflect the current state of the dataset.

    Args:
        path_export (str): The file path to the dataset that needs to be updated.
        dataset_name (str): The name of the dataset, used for the FTP destination path.
        dataset_id (str): The ID of the dataset to be published on data.bs.ch.
    """
    if ct.has_changed(path_export):
        common.upload_ftp(path_export, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass,
                          f'parlamentsdienst/gr_{dataset_name}')
        odsp.publish_ods_dataset_by_id(dataset_id)
        ct.update_hash_file(path_export)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful')
