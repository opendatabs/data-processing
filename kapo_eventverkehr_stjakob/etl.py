import logging
import os

import pandas as pd


def main():
    file_path = os.path.join("data_orig", "Events St. Jakob.xlsx")
    df = pd.read_excel(file_path, sheet_name="Eventliste")
    df.to_csv(os.path.join("data", "events_stjakob.csv"), index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
