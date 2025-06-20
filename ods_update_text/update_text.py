import logging

import ods_utils_py as ods_utils

import common

# The skript modifies text in the metadata of public datasets. It requires the parameters 'new_text' and 'old_text'


def replace_text_in_metadata(metadata, old_text: str, new_text: str):
    """
    Recursively iterates through the values in the metadata and replaces text
    """
    if not isinstance(metadata, dict):
        raise TypeError("metadata must be a dictionary!")  # Make sure metadata is a dict

    def recursive_replace(value):
        if isinstance(value, dict):  # If Dictionary, check all values
            return {key: recursive_replace(val) for key, val in value.items()}
        elif isinstance(value, list):  # If list, check all elements
            return [recursive_replace(item) for item in value]
        elif isinstance(value, str):  # If string, replace
            return value.replace(old_text, new_text)
        else:  # Leave all other types unchanged
            return value

    return recursive_replace(metadata)


base_url = "https://data.bs.ch/api/automation/v1.0/"


def main():
    ids_url = "https://data.bs.ch/api/explore/v2.1/catalog/datasets/100057/exports/json?select=dataset_identifier&limit=-1&timezone=UTC&use_labels=false&epsg=4326"

    logging.info("get the IDs of the records")
    r = ods_utils.requests_get(ids_url)
    response_data = r.json()
    ids = [id["dataset_identifier"] for id in response_data]

    for id in ids:
        try:
            metadata = ods_utils.get_dataset_metadata(dataset_id=id)
            if not metadata:
                logging.info(f"No metadata found for ID {id}")
                continue

            # Replace text
            new_text = "Open Data Basel-Stadt"
            old_text = ("Fachstelle für OGD Basel-Stadt",)
            updated_metadata = replace_text_in_metadata(metadata, old_text=old_text, new_text=new_text)

            # Get the uid of dataset
            dataset_uid = common.get_ods_uid_by_id(id, common.credentials)

            # Update the dataset ( Update + Publish)
            r = ods_utils.requests_put(url=f"{base_url}/datasets/{dataset_uid}/metadata/", json=updated_metadata)
            ods_utils.set_dataset_public(dataset_uid=dataset_uid)

            if r.status_code == 200:
                logging.info(f"Successfully updated: {id}")
            else:
                logging.info(f"Error in {id}: {r.status_code} - {r.text}")
        except Exception as e:
            logging.info(f"Error in {id}: {str(e)}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
    logging.info("Job completed successfully!")
