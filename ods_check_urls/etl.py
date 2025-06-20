import logging
import os
import pathlib

import pandas as pd


# TODO: Rewrite this job to check all urls in all datasets and send an email if a url has changed or is not reachable
def main():
    df_urls = pd.read_excel(
        os.path.join(pathlib.Path(__file__).parent.absolute(), "data_orig", "urls_to_replace_ods.xlsx")
    )
    df_urls["dataset_ids_references"] = df_urls["dataset_ids_references"].astype(str)
    df_urls["dataset_ids_description"] = df_urls["dataset_ids_description"].astype(str)

    df_stata_harvester_ids = pd.read_excel(
        os.path.join(pathlib.Path(__file__).parent.absolute(), "data_orig", "urls_to_replace_ods.xlsx"),
        sheet_name="stata_harvester_ids",
    )
    stata_ids = df_stata_harvester_ids["stata_harvester_ids"].astype(str).tolist()
    df_urls["stata_harvester_ids_references"] = df_urls["dataset_ids_references"].apply(
        lambda x: find_matching_ids(x, stata_ids)
    )
    df_urls["stata_harvester_ids_description"] = df_urls["dataset_ids_description"].apply(
        lambda x: find_matching_ids(x, stata_ids)
    )

    df_gva_harvester_ids = pd.read_excel(
        os.path.join(pathlib.Path(__file__).parent.absolute(), "data_orig", "urls_to_replace_ods.xlsx"),
        sheet_name="gva_harvester_ids",
    )
    gva_ids = df_gva_harvester_ids["gva_harvester_ids"].astype(str).tolist()
    df_urls["gva_harvester_ids_references"] = df_urls["dataset_ids_references"].apply(
        lambda x: find_matching_ids(x, gva_ids)
    )
    df_urls["gva_harvester_ids_description"] = df_urls["dataset_ids_description"].apply(
        lambda x: find_matching_ids(x, gva_ids)
    )

    df_urls.to_excel(
        os.path.join(pathlib.Path(__file__).parent.absolute(), "data", "urls_to_replace_ods.xlsx"), index=False
    )


def find_matching_ids(reference_string, stata_ids):
    # Check for NaN values
    if reference_string == "nan":
        return ""
    # Split the reference_string by comma and trim spaces
    ids_in_reference = [x.strip() for x in reference_string.split(",")]
    # Find intersection with stata_ids and join into a string
    matching_ids = [x for x in ids_in_reference if x in stata_ids]
    return ", ".join(matching_ids)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job successful!")
