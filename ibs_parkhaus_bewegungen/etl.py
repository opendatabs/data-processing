import logging
import os
import pathlib

import common
import pandas as pd


def main():
    dfs = []
    # Iterate over credentials.data_file_root and read the data files
    for file in os.listdir("data_orig"):
        file_name = os.path.join("data_orig", file)
        logging.info(f"Parsing file {file_name}...")
        engine = "openpyxl" if file_name.endswith(".xlsx") else "xlrd"
        df = pd.read_excel(
            file_name,
            skiprows=7,
            usecols="A:D",
            header=None,
            engine=engine,
            names=["title", "timestamp_text", "einfahrten", "ausfahrten"],
        )
        df = df.dropna(subset=["ausfahrten"])
        df["timestamp"] = (
            pd.to_datetime(df.timestamp_text, format="%Y-%m-%d %H:%M:%S")
            .dt.tz_localize(tz="Europe/Zurich", ambiguous=True)
            .dt.tz_convert("UTC")
        )
        logging.info("Adding rows with no data...")
        df = df.set_index("timestamp").asfreq("1H").reset_index()
        df["timestamp_text"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
        df[["einfahrten", "ausfahrten"]] = df[["einfahrten", "ausfahrten"]].fillna(0)
        df.title = pd.read_excel(
            file_name, skiprows=1, usecols="A", header=None, engine=engine
        ).iloc[0, 0]
        dfs.append(df)
    all_df = pd.concat(dfs)
    all_df = all_df.convert_dtypes()
    export_filename = os.path.join(
        pathlib.Path(__file__).parent, "data", "parkhaus_bewegungen.csv"
    )
    all_df.to_csv(export_filename, index=False)
    common.update_ftp_and_odsp(export_filename, "ibs_parkhaus_bewegungen", "100198")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successfully completed!")
