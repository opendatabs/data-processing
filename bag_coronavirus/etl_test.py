import logging
from bag_coronavirus import credentials
import os
import common
import pandas as pd


def main():
    print(f"Getting today's data url...")
    context_json = common.requests_get(url='https://www.covid19.admin.ch/api/data/context').json()
    csv_daily_tests_url = context_json['sources']['individual']['csv']['daily']['test']
    print(f'Reading current csv from {csv_daily_tests_url} into data frame...')
    df = pd.read_csv(common.get_text_from_url(csv_daily_tests_url))
    print(f'Filtering out BS rows, some columns, and rename them...')
    df_bs = df.query('geoRegion == "BS"')
    df_bs = df_bs.filter(items=['datum', 'entries_neg', 'entries_pos', 'entries'])
    #df_bs['positivity_rate_percent'] = df_bs['entries_pos'] / df_bs['entries'] * 100
    #df_bs['positivity_rate'] = df_bs['entries_pos'] / df_bs['entries']
    df_bs = df_bs.rename(columns={'entries_neg': 'negative_tests', 'entries_pos': 'positive_tests', 'entries': 'total_tests'})
    print(f'Calculating columns...')
    df_bs['dayofweek'] = pd.to_datetime(df_bs['datum']).dt.dayofweek + 1
    df_bs['weekday_nr'] = pd.to_datetime(df_bs['datum']).dt.dayofweek
    df_bs['woche'] = pd.to_datetime(df_bs['datum']).dt.isocalendar().week

    export_file_name = os.path.join(credentials.path, credentials.file_name)
    print(f'Exporting to file {export_file_name}...')
    df_bs.to_csv(export_file_name, index=False)
    common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag_coronavirus_tests')

    pcr_antigen_path = os.path.join(credentials.path, 'covid19_testPcrAntigen.csv')
    print(f'Reading pcr/antigen csv from {pcr_antigen_path} into data frame...')
    df_pcr_antigen = pd.read_csv(pcr_antigen_path)
    # df_type = df_pcr_antigen[['datum', 'entries', 'entries_neg', 'entries_pos', 'nachweismethode', 'geoRegion']]
    df_type = df_pcr_antigen[['datum', 'entries', 'geoRegion']]
    df_type_bs = df_type.query("geoRegion == 'BS'").copy(deep=False)
    # df_type_bs['positivity_rate'] = df_type_bs.entries_pos / df_type_bs.entries
    # df_type_bs['positivity_rate_percent'] = df_type_bs.positivity_rate * 100
    # df_pivot = df_type_bs.pivot_table(index=['datum', 'geoRegion'], columns=['nachweismethode'], values=['entries', 'entries_neg', 'entries_pos', 'positivity_rate', 'positivity_rate_percent'])
    # Replace the 2-level column names with a string that concatenates both strings
    df_pivot = df_type_bs.pivot_table(index=['datum', 'geoRegion'], values=['entries'])
    df_pivot.columns = ["_".join(str(c) for c in col) for col in df_pivot.columns.values]
    df_pivot = df_pivot.reset_index()
    df_pivot = df_pivot.drop(columns=['geoRegion'])
    df_bs_merged = df_bs.merge(df_pivot, how='left', on='datum')
    df_bs_merged = df_bs_merged.sort_values(by='datum', ascending=False)

    export_file_name_merged = os.path.join(credentials.path, 'covid19_tests_bs_all_nachweismethode.csv')
    print(f'Exporting file with data per nachweismethode added to file {export_file_name_merged}...')
    df_bs_merged.to_csv(export_file_name_merged, index=False)
    common.upload_ftp(export_file_name_merged, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'bag')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
