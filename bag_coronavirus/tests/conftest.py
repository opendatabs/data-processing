import os
import pandas as pd
import pytest
from bag_coronavirus.src import etl_impftermine, etl_vmdl_altersgruppen, vmdl, etl_vmdl_impf_uebersicht

CURR_DIR = os.path.dirname(os.path.realpath(__file__))
IMPFT_DF_FILE = os.path.join(CURR_DIR, 'fixtures', 'impftermine_df.pickle')
IMPFT_DF_AGG_FILE = os.path.join(CURR_DIR, 'fixtures', 'impftermine_df_agg.pickle')
VMDL_CSV_FILE = os.path.join(CURR_DIR, 'fixtures', 'vmdl.csv')
VMDL_RAW_DF = os.path.join(CURR_DIR, 'fixtures', 'vmdl_df_bs_long_all.pickle')
VMDL_REPORTING_DF = os.path.join(CURR_DIR, 'fixtures', 'vmdl_reporting_df.pickle')
VMDL_IMPF_OVERVIEW_DF = os.path.join(CURR_DIR, 'fixtures', 'vmdl_overview_df.pickle')


@pytest.fixture
def impft_df():
    return pd.read_pickle(IMPFT_DF_FILE)


def persist_impft_df():
    df = etl_impftermine.load_data()
    df.to_pickle(IMPFT_DF_FILE)


@pytest.fixture
def impft_df_agg():
    return pd.read_pickle(IMPFT_DF_AGG_FILE)


def persist_impft_df_agg():
    df = etl_impftermine.load_data()
    df, df_agg = etl_impftermine.transform(df)
    df_agg.to_pickle(IMPFT_DF_AGG_FILE)


@pytest.fixture
def vmdl_raw_df():
    return pd.read_pickle(VMDL_RAW_DF)


def persist_vmdl_raw_df():
    df_bs_long_all = etl_vmdl_altersgruppen.get_raw_df(file_path=VMDL_CSV_FILE)
    df_bs_long_all.to_pickle(VMDL_RAW_DF)


@pytest.fixture
def vmdl_reporting_df():
    return pd.read_pickle(VMDL_REPORTING_DF)


def persist_vmdl_reporting_df():
    df_bs_long_all = etl_vmdl_altersgruppen.get_raw_df(file_path=VMDL_CSV_FILE)
    df_bs_perc = etl_vmdl_altersgruppen.get_reporting_df(df_bs_long_all)
    df_bs_perc.to_pickle(VMDL_REPORTING_DF)


@pytest.fixture()
def vmdl_impf_overview_df():
    return pd.read_pickle(VMDL_IMPF_OVERVIEW_DF)


def persist_vmdl_impf_overview_df():
    df = etl_vmdl_impf_uebersicht.extract_data(VMDL_CSV_FILE)
    export_df = etl_vmdl_impf_uebersicht.transform_data(df)
    export_df.to_pickle(VMDL_IMPF_OVERVIEW_DF)


def persists_vmdl_csv():
    vmdl.retrieve_vmdl_data(VMDL_CSV_FILE)







def main():
    # persists_vmdl_csv()
    # persist_vmdl_raw_df()
    # persist_vmdl_reporting_df()
    persist_vmdl_impf_overview_df()
    pass


if __name__ == "__main__":
    print(f'Executing {__file__}...')
    main()
