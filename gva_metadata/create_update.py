import requests
import pandas as pd
from gva_metadata import credentials
import os
import logging

# Base URL of your Open DataSoft instance
base_url = "https://data.bs.ch/api/automation/v1.0/datasets/"
get_url = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/"
api_key = credentials.api_key

headers = {
    "Authorization": f"Apikey {api_key}",
    "Content-Type": "application/json",
}


def main():
    file_name = "gva_metadata.csv"
    file_path = os.path.join(credentials.file_path, file_name)
    metadata_df = pd.read_csv(file_path, sep=";", encoding="utf-8")
    # Reformat date to the correct format
    if "modified" in metadata_df.columns:
        metadata_df["modified"] = pd.to_datetime(metadata_df["modified"], format="%d.%m.%Y").dt.strftime("%Y-%m-%d")

    # Go through CSV data and process records
    for _, row in metadata_df.iterrows():
        dataset_id = row["title"].lower().replace(" ", "_")  # Generate ID from title
        api_url = f"{base_url}{dataset_id}"
        # Metadaten-Payload
        payload = {
            "dataset_id": dataset_id,
            "is_restricted": False,
            "metadata": {
                "default": {
                    "title": {"value": row["title"]},
                    "description": {"value": row["description"]},
                    "language": {"value": row["language"]},
                    "attributions": {
                        "value": [row["attributions"]]},
                    "tags":  {"value": row["tags"].split(";") if pd.notna(row["tags"]) else []},
                    "modified": {"value": row["modified"]},
                },
                "custom": {
                    "tags": {"value": ["Tag1", "Tag"]}
                }
            },
        }

        # Check whether the record exists
        check_url = f"{get_url}/{dataset_id}"
        check_response = requests.get(check_url, headers=headers, proxies=credentials.proxies)
        if check_response.status_code == 200:
            response = requests.put(api_url, headers=headers, json=payload, proxies=credentials.proxies)
            logging.info("🔄 Aktualisiert")
        else:
            response = requests.post(base_url, headers=headers, json=payload, proxies=credentials.proxies)
            logging.info("🆕 Erstellt")

    if __name__ == "__main__":
        logging.basicConfig(level=logging.DEBUG)
        logging.info(f'Executing {__file__}...')
        main()
        logging.info('🎉 Job successful!')
