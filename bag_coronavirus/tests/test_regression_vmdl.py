import pytest
from bag_coronavirus.src import vmdl, etl_vmdl_altersgruppen
import pandas as pd
import conftest


def test_regression_vmdl_raw(vmdl_raw_df):
    raw_df = etl_vmdl_altersgruppen.get_raw_df(conftest.VMDL_CSV_FILE, etl_vmdl_altersgruppen.get_age_group_periods())
    assert vmdl_raw_df.equals(raw_df)


def test_regression_vmdl_reporting(vmdl_reporting_df):
    raw_df = etl_vmdl_altersgruppen.get_raw_df(conftest.VMDL_CSV_FILE, etl_vmdl_altersgruppen.get_age_group_periods())
    rep_df = etl_vmdl_altersgruppen.get_reporting_df(raw_df, etl_vmdl_altersgruppen.get_age_group_periods())
    assert rep_df.equals(vmdl_reporting_df)


def test_first_age_group_period_regression(vmdl_raw_df):
    bin_def = etl_vmdl_altersgruppen.get_age_group_periods()[0]
    raw_df = etl_vmdl_altersgruppen.get_partial_raw_df(conftest.VMDL_CSV_FILE, bin_def)
    until_date = bin_def['until_date']
    assert vmdl_raw_df.query('vacc_day <= @until_date').equals(raw_df), 'First age_group period should match with older data where only 1 period was present.'