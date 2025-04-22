import json
import logging
import os
import warnings
from datetime import datetime

import common
import pandas as pd
import urllib3
from common import change_tracking as ct
from more_itertools import chunked

FTP_SERVER = os.getenv("FTP_SERVER")
FTP_USER = os.getenv("FTP_USER_07")
FTP_PASS = os.getenv("FTP_PASS_07")
ODS_PUSH_URLS = json.loads(os.getenv("ODS_PUSH_URLS_LUFTQUALITAET_CH", "{}"))


def main():
    today_string = datetime.today().strftime("%d.%m.%Y")
    decades = ["01.01.2000", "01.01.2010", "01.01.2020", today_string]
    for i in range(len(decades) - 1):
        logging.info(
            f"Processing data for the period {decades[i]} - {decades[i + 1]}..."
        )

        base_payload = {
            "jsform": "true",
            "querytype": "station",
            "station_interval": "hour",
            "station_output": "csv",
            "pollutant_interval": "hour",
            "pollutant_output": "interactive",
            "timerange": "custom",
            "startdate": decades[i],
            "stopdate": decades[i + 1],
            "submit": "Abfrage",
        }
        station_payload = [
            {
                "station": "bsBET",  # Chrischona Bettingen
                "pollutants[]": ["O3"],
                "ods_id": "100048",
            },
            {
                "station": "bsBSJ",  # St. Johannplatz
                "pollutants[]": ["PM10", "PM2.5", "O3", "NO2"],
                "ods_id": "100049",
            },
            {
                "station": "bsBFB",  # Feldbergstrasse
                "pollutants[]": ["PM10", "PM2.5", "NO2"],
                "ods_id": "100050",
            },
        ]

        for station in station_payload:
            station_abbrev = station["station"]
            logging.info(f"Handling station {station_abbrev}...")
            # merge dicts, see e.g. https://towardsdatascience.com/merge-dictionaries-in-python-d4e9ce137374
            payload = station | base_payload
            logging.info("Requesting data from web service...")
            urllib3.disable_warnings()
            r = common.requests_post(
                url="https://luftqualitaet.ch/messdaten/datenarchiv/abfrage",
                data=payload,
                verify=False,
            )
            warnings.resetwarnings()

            raw_file = os.path.join(
                "data", f"Luftqualitaet_ch-{station_abbrev}-{decades[i][-4:]}-raw.csv"
            )
            logging.info(f"Writing data into file {raw_file}...")
            with open(raw_file, "wb") as fd:
                for chunk in r.iter_content(chunk_size=128):
                    fd.write(chunk)

            if ct.has_changed(raw_file):
                logging.info("Reading csv into df...")
                # Some lines have more than 5 columns, ignoring those.
                df = pd.read_csv(
                    raw_file,
                    skiprows=5,
                    encoding="cp1252",
                    sep=";",
                    on_bad_lines="skip",
                )
                df = df.rename(columns={})
                logging.info("Removing empty lines...")
                cols = df.columns.to_list()
                data_cols = list(filter(lambda item: item not in ["Datum/Zeit"], cols))
                df = df.dropna(how="all", subset=data_cols).reset_index(drop=True)
                logging.info("Renaming columns...")
                df = df.rename(
                    columns={
                        "Datum/Zeit": "datum_zeit",
                        "PM10 (Stundenmittelwerte  [µg/m³])": "pm10_stundenmittelwerte_ug_m3",
                        "O3 (Stundenmittelwerte  [µg/m³])": "o3_stundenmittelwerte_ug_m3",
                        "NO2 (Stundenmittelwerte  [µg/m³])": "no2_stundenmittelwerte_ug_m3",
                        "PM2.5 (Stundenmittelwerte  [µg/m³])": "pm2_5_stundenmittelwerte_ug_m3",
                    }
                )
                export_file = os.path.join(
                    "data", f"Luftqualitaet_ch-{station_abbrev}-{decades[i][-4:]}.csv"
                )
                df.to_csv(export_file, index=False)
                common.upload_ftp(
                    export_file, FTP_SERVER, FTP_USER, FTP_PASS, "luftqualitaet_ch"
                )
                if ct.has_changed(export_file):
                    chunk_size = 25000
                    df_chunks = chunked(df.index, chunk_size)
                    for df_chunk_indexes in df_chunks:
                        logging.info("Submitting a data chunk to ODS...")
                        df_chunk = df.iloc[df_chunk_indexes]
                        df_json = df_chunk.to_json(orient="records")
                        # print(f'Pushing the following data to ODS: {json.dumps(json.loads(payload), indent=4)}')
                        # use data=payload here because payload is a string. If it was an object, we'd have to use json=payload.
                        urllib3.disable_warnings()
                        rq = common.requests_post(
                            url=ODS_PUSH_URLS[station_abbrev],
                            data=df_json,
                            verify=False,
                        )
                        warnings.resetwarnings()
                        rq.raise_for_status()
                    ct.update_hash_file(export_file)
                ct.update_hash_file(raw_file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
