import os

import pandas as pd
import pytest

from bag_coronavirus.src import etl_impftermine

CURR_DIR = os.path.dirname(os.path.realpath(__file__))
DF_FILE = os.path.join(CURR_DIR, 'fixtures', 'impftermine_df.pickle')
DF_AGG_FILE = os.path.join(CURR_DIR, 'fixtures', 'impftermine_df_agg.pickle')


@pytest.fixture
def df():
    return pd.read_pickle(DF_FILE)


@pytest.fixture
def df_agg():
    return pd.read_pickle(DF_AGG_FILE)


def persist_df():
    df = etl_impftermine.load_data()
    df.to_pickle(DF_FILE)


def persist_df_agg():
    df = etl_impftermine.load_data()
    df, df_agg = etl_impftermine.transform(df)
    df_agg.to_pickle(DF_AGG_FILE)
