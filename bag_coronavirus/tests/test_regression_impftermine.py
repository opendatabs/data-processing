import pytest
import pandas as pd
import os
from bag_coronavirus.src import etl_impftermine
from bag_coronavirus.tests import conftest


def test_regression_impftermine(impft_df, impft_df_agg):
    df_to_test, df_agg_to_test = etl_impftermine.transform(impft_df)
    assert impft_df_agg.equals(df_agg_to_test)


def main():
    # print(df_agg())
    # persist_df_agg()
    pass


if __name__ == "__main__":
    pytest.main()
    # main()
