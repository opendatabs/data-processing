import pytest
import pandas as pd
import os
from bag_coronavirus.src import etl_impftermine
from bag_coronavirus.tests import conftest


def test_regression(df, df_agg):
    df_to_test, df_agg_to_test = etl_impftermine.transform(df)
    assert df_agg.equals(df_agg_to_test)


def main():
    # print(df_agg())
    # persist_df_agg()
    pass


if __name__ == "__main__":
    pytest.main()
    # main()
