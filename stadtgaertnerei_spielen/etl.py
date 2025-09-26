import logging
import os
from io import StringIO

import common
import pandas as pd
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

URL_SPIELPLAETZE = os.getenv("HTTPS_URL_TBA_SPIELPLAETZE")
URL_SPIELGERAETE = os.getenv("HTTPS_URL_TBA_SPIELGERAETE")
USER = os.getenv("HTTPS_USER_TBA")
PASS = os.getenv("HTTPS_PASS_TBA")

def get_spielplaetze():
    r = common.requests_get(url=URL_SPIELPLAETZE, auth=HTTPBasicAuth(USER, PASS))
    if len(r.text) == 0:
        logging.error("No data retrieved from API!")
        raise RuntimeError("No data retrieved from API.")
    else:
        df = pd.read_json(StringIO(r.text))
        return df


def get_spielgeraete():
    r = common.requests_get(url=URL_SPIELGERAETE, auth=HTTPBasicAuth(USER, PASS))
    if len(r.text) == 0:
        logging.error("No data retrieved from API!")
        raise RuntimeError("No data retrieved from API.")
    else:
        df = pd.read_json(StringIO(r.text))
        return df


def main():
    df_spielplaetze = get_spielplaetze()
    path_spielplaetze = "data/100462_spielplaetze.csv"
    df_spielplaetze.to_csv(path_spielplaetze, index=False)
    common.update_ftp_and_odsp(path_spielplaetze, "stadtgaertnerei/spielen", "100462")

    
    df_spielgeraete = get_spielgeraete()
    path_spielgeraete = "data/100463_spielgeraete.csv"
    df_spielgeraete.to_csv(path_spielgeraete, index=False)
    common.update_ftp_and_odsp(path_spielgeraete, "stadtgaertnerei/spielen", "100463")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
