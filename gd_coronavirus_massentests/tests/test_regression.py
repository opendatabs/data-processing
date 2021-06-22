import pytest
import pandas as pd
from gd_coronavirus_massentests.src import etl
import glob
import os
from gd_coronavirus_massentests.tests import conftest


def test_regression(single_df, pool_df):
    date, dfs = etl.extract_db_data(glob.glob(os.path.join(conftest.CURR_DIR, 'fixtures/workflow_data', "*.zip")))
    dfs['df_lab'] = etl.extract_lab_data(os.path.join(conftest.CURR_DIR, 'fixtures/lab_data', "*.xml"))
    etl.add_global_dfs(dfs)
    etl.convert_datetime_columns(dfs)
    massentests_pool = etl.calculate_report(etl.get_report_defs()[0]['table_name'])
    massentests_single = etl.calculate_report(etl.get_report_defs()[1]['table_name'])

    # Persist pickle to compare to later
    # pool_pickle_path = os.path.join(conftest.CURR_DIR, 'fixtures/massentests_pool.pickle')
    # massentests_pool.to_pickle(pool_pickle_path)
    # single_pickle_path = os.path.join(conftest.CURR_DIR, 'fixtures/massentests_single.pickle')
    # massentests_single.to_pickle(single_pickle_path)

    assert massentests_pool.equals(pool_df)
    # assert massentests_single.drop(columns=['BusinessCount']).equals(single_df)
    assert massentests_single.equals(single_df)


def main():
    pass


if __name__ == "__main__":
    pytest.main()
    # main()
