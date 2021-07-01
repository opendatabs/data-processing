import pytest
import pandas as pd
import os
from bag_coronavirus.src import etl_impftermine, etl_vmdl_altersgruppen
from bag_coronavirus.tests import conftest


def test_regression_impftermine(impft_df, impft_df_agg):
    df_to_test, df_agg_to_test = etl_impftermine.transform(impft_df)
    assert impft_df_agg.equals(df_agg_to_test)


def test_first_age_group_period_regression(impft_df, impft_df_agg):
    df_to_test, df_agg_to_test = etl_impftermine.transform(impft_df)
    bin_def = etl_vmdl_altersgruppen.get_age_group_periods()[0]
    until_date = bin_def['until_date']
    assert df_agg_to_test.query('date <= @until_date').equals(impft_df_agg.query('date <= @until_date')), 'First age_group period should match with older data where only 1 period was present.'


def main():
    # print(df_agg())
    # persist_df_agg()
    pass


if __name__ == "__main__":
    pytest.main()
    # main()
