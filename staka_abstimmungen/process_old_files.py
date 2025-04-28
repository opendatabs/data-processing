import os
import pathlib

import numpy as np
import pandas as pd

from etl_details import calculate_details
from etl_kennzahlen import calculate_kennzahlen

"""
script to compare the output of etl_all.py with the result of newly processing the data 
"""


def bring_in_right_form_to_compare(df):
    df.sort_values(
        by=["Abst_Datum", "Abst_Titel", "Gemein_ID", "Wahllok_ID", "Abst_ID"],
        inplace=True,
    )
    df = df.reindex(sorted(df.columns), axis=1)
    df.reset_index(drop=True, inplace=True)
    return df


# process old data files
path_files = os.path.join(
    pathlib.Path(__file__).parents[1], "data/Schlussresultate_abst"
)
files = [
    f for f in os.listdir(path_files) if os.path.isfile(os.path.join(path_files, f))
]

dates = set()
for file in files:
    date_str = os.path.basename(file).split("_", -1)[-1][:8]
    dates.add(date_str)

df_test = pd.DataFrame()
for date in dates:
    files_date = [file for file in files if date in file]
    _, df_kennz = calculate_kennzahlen(files_date)
    _, df_det = calculate_details(files_date)
    df_test = pd.concat([df_test, df_kennz, df_det])


# compare data from dataportal with newly processed data
path_all = "/Users/hester/PycharmProjects/data-processing/staka_abstimmungen/src/dataset_process_harmonize_old_datasets.csv"
df_dataportal_all = pd.read_csv(path_all)
df_dataportal_all = bring_in_right_form_to_compare(df_dataportal_all)
df_test = bring_in_right_form_to_compare(df_test)
for x in df_dataportal_all.columns:
    df_test[x] = df_test[x].astype(df_dataportal_all[x].dtypes.name)


diff = df_test.compare(df_dataportal_all)
diff["Datum"] = df_test.Abst_Datum

float_columns = [x for x in df_test.columns if df_test[x].dtypes == "float64"]
other_columns = [x for x in df_test.columns if x not in float_columns]

df_test_float = df_test[float_columns]
df_dataportal_all_float = df_dataportal_all[float_columns]

diff_float = pd.DataFrame(
    np.isclose(df_test_float, df_dataportal_all_float),
    columns=df_test_float.columns,
    index=df_test_float.index,
)
