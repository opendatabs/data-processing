import os
from pathlib import Path

import pandas as pd
import pyodbc
from dotenv import load_dotenv

load_dotenv()

PATH_TO_DSN = os.getenv("PATH_TO_DSN")


def main():
    conn = pyodbc.connect(PATH_TO_DSN, autocommit=True)

    df = pd.read_sql("SELECT * FROM polizei.FaktEinsaetze", conn)
    df.to_csv(Path("data_orig/polizei_FaktEinsaetze.csv"), index=False)


if __name__ == "__main__":
    main()
