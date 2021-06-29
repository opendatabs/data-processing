import pytest
from bag_coronavirus.src import vmdl, etl_vmdl_altersgruppen
import pandas as pd
import conftest


def test_regression_vmdl_raw(vmdl_raw_df):
    raw_df = etl_vmdl_altersgruppen.get_raw_df(conftest.VMDL_CSV_FILE)
    assert vmdl_raw_df.equals(raw_df)


def test_regression_vmdl_reporting(vmdl_reporting_df):
    raw_df = etl_vmdl_altersgruppen.get_raw_df(conftest.VMDL_CSV_FILE)
    rep_df = etl_vmdl_altersgruppen.get_reporting_df(raw_df)
    assert rep_df.equals(vmdl_reporting_df)
