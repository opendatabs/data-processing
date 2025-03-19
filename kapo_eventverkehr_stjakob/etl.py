import os
import logging
import pandas as pd

import common
from dotenv import load_dotenv

load_dotenv()

DATA_ORIG_PATH = os.getenv("DATA_ORIG_PATH")

def main():
    file_path = os.path.join(DATA_ORIG_PATH, 'Events St. Jakob.xlsx')
    df = pd.read_excel(file_path, sheet_name='Eventliste')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
