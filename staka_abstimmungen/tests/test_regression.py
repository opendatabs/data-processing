import pandas as pd
import pytest
from staka_abstimmungen.src import etl_kennzahlen
from staka_abstimmungen import credentials
import os
import filecmp

CURR_DIR = os.path.dirname(os.path.realpath(__file__))


def test_regression():
    ref_file = os.path.join(CURR_DIR, 'fixtures', 'Abstimmungen_2021-06-13.csv')
    source_files = [os.path.join(CURR_DIR, 'fixtures', 'Resultate_EID.xlsx'), os.path.join(CURR_DIR, 'fixtures', 'Resultate_KAN.xlsx')]
    abst_date, df_report = etl_kennzahlen.calculate_kennzahlen(source_files)
    file_to_test = os.path.join(CURR_DIR, 'fixtures', 'kennzahlen_generated_by_test.csv')
    df_report.to_csv(file_to_test, index=False)
    assert filecmp.cmp(ref_file, file_to_test)

