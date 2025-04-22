import os

import pandas as pd

file_path_orig = os.path.join("data_orig", "ogd_datensaetze.csv")
print(f"Reading current data file {file_path_orig}...")
current_data = pd.read_csv(file_path_orig, sep=";", na_filter=False, encoding="cp1252")

file_path_last = os.path.join("data", "last_ogd_datensaetze.csv")
print(f"Reading last data file {file_path_last}...")
last_data = pd.read_csv(file_path_last, sep=";", na_filter=False, encoding="cp1252")

print("Comparing dataframes...")
differences = current_data.compare(last_data, keep_equal=False)
# print(differences)

# comparison_values = current_data == last_data
# print(comparison_values)

# ideas:
# https://pypi.org/project/csv-diff/
# https://pypi.org/project/csvdiff/
# https://pypi.org/project/daff/

# https://kanoki.org/2019/02/26/compare-two-excel-files-for-difference-using-python/
# https://kanoki.org/2019/07/04/pandas-difference-between-two-dataframes/

# email with diff

print("Copying current data file as base line for tomorrow's comparison...")
# shutil.copy2(file_path_orig, file_path_last)
