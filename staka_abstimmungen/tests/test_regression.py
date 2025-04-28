import filecmp
import os

import etl_details
import etl_kennzahlen

CURR_DIR = os.path.dirname(os.path.realpath(__file__))


def test_regression_kennzahlen(tmp_path):
    ref_file = os.path.join(CURR_DIR, "fixtures", "Abstimmungen_2021-06-13.csv")
    source_files = [
        os.path.join(CURR_DIR, "fixtures", "Resultate_EID.xlsx"),
        os.path.join(CURR_DIR, "fixtures", "Resultate_KAN.xlsx"),
    ]

    abst_date, df_report = etl_kennzahlen.calculate_kennzahlen(source_files)
    file_to_test = os.path.join(
        CURR_DIR, "fixtures", f"Abstimmungen_{abst_date}_generated_by_test.csv"
    )
    df_report.to_csv(file_to_test, index=False)
    assert filecmp.cmp(ref_file, file_to_test)


def test_regression_details(tmp_path):
    ref_file = os.path.join(CURR_DIR, "fixtures", "Abstimmungen_Details_2021-06-13.csv")
    source_files = [
        os.path.join(CURR_DIR, "fixtures", "Resultate_EID.xlsx"),
        os.path.join(CURR_DIR, "fixtures", "Resultate_KAN.xlsx"),
    ]

    abst_date, df_report = etl_details.calculate_details(source_files)
    file_to_test = os.path.join(
        CURR_DIR, "fixtures", f"Abstimmungen_Details_{abst_date}_generated_by_test.csv"
    )
    df_report.to_csv(file_to_test, index=False)
    assert filecmp.cmp(ref_file, file_to_test)
