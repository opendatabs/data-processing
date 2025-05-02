import pytest
from bag_coronavirus.src import vmdl, etl_vmdl_impf_uebersicht
import pandas as pd
import conftest


def test_regression_vmdl_raw(vmdl_impf_overview_df):
    df = etl_vmdl_impf_uebersicht.extract_data(conftest.VMDL_CSV_FILE)
    df_export = etl_vmdl_impf_uebersicht.transform_data(df)
    assert vmdl_impf_overview_df.equals(df_export)


def test_new_cols_regression_vmdl_raw(vmdl_impf_overview_df):
    df = etl_vmdl_impf_uebersicht.extract_data(conftest.VMDL_CSV_FILE)
    df_export = etl_vmdl_impf_uebersicht.transform_data(df)
    df_export_2 = df_export[vmdl_impf_overview_df.columns]
    assert vmdl_impf_overview_df.equals(df_export_2)
