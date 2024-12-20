"""
This class updates the temporal coverage fields of all datasets with unrestricted access by automatically detecting
date or datetime columns. It sorts these columns by granularity and only considers those with the highest granularity
available. Specifically:

- If datetime fields are present, only these are considered.
- If no datetime fields are found, only date fields with 'day' granularity are used.
- If no fields with 'day' granularity are available, fields with 'month' granularity are used.
- If no 'month' fields are present, fields with 'year' granularity are used.

If none of these granularities are present, the process skips the dataset and moves to the next.
"""

import os
import logging
from typing import Optional, Dict, Any

import requests.exceptions
from dateutil.relativedelta import relativedelta
import pandas as pd

import ods_utils_py as ods_utils
from ods_utils_py import _requests_utils

from datetime import datetime


def _parse_date(date: str, is_min_date: bool) -> Optional[datetime]:
    """
    Creates a datetime object in canonical form. Does not consider time, and always rounds the date to day-precision.
    When given only the year, e.g. 2020, this will result in 2020-01-01 (min_date) or 2020-12-31 (max_date),
    When given only the month, e.g. 2016-05, this will result in 2016-05-01 (min_date) or 2016-05-31 (max_date).

    Args:
        date: The date to parse in the form %Y-%m-%d, %Y-%m or %Y. Can also handle more granular dates, for example
            the date 2018-06-01T00:00:00+00:00 will be handled like 2018-06-01
        is_min_date: If is_min_date is False, then it is a max_date. This is relevant for rounding, e.g. whether 2024-03
            gets rounded to 2024-03-01 (min_date) or 2024-03-31 (max_date)

    Returns: A datetime object
    """

    date_no_time = date.split('T')[0]

    match len(date.split('-')):
        case 3:  # Day precision
            dt = datetime.strptime(date_no_time, '%Y-%m-%d')
            return dt

        case 2:  # Month precision
            dt = datetime.strptime(date_no_time, '%Y-%m')
            if not is_min_date:
                dt = dt + relativedelta(months=1) - relativedelta(days=1)
            return dt

        case 1:  # Year precision
            dt = datetime.strptime(date_no_time, '%Y')
            if not is_min_date:
                dt = dt + relativedelta(years=1) - relativedelta(days=1)
            return dt

        case _:
            raise ValueError(f"Date format for '{date}' not recognized")


def get_dataset_date_range(dataset_id: str) -> (str, str, Dict[str, Any]):
    """
    Find the oldest and newest date in the dataset. This will only consider columns that are of the format datetime or
    date.

    Args:
        dataset_id: The id of the dataset that can be seen in the url of the dataset

    Returns:
        A tuple containing:
            - min_date_str (Optional[str]): The oldest date in the dataset formatted as YYYY-MM-DD, or None if not found.
            - max_date_str (Optional[str]): The newest date in the dataset formatted as YYYY-MM-DD, or None if not found.
            - additional_information (Dict[str, Any]): A dictionary containing additional information such as:
                - "date_fields_found" (List[str]): List of all date or datetime fields found.
                - "date_fields_considered" (List[str]): List of date or datetime fields considered for analysis.
                - "granularity_used" (str): The granularity level used ("datetime", "day", "month", "year", or "").
                - "status" (str): Indicates if the process was successful or if there were any issues.
    """

    base_url = "https://data.bs.ch/api/explore/v2.1"

    additional_information: Dict[str, Any] = {
        "date_fields_found": [],
        "date_fields_considered": [],
        "granularity_used": "",
        "status": "No suitable fields found"
    }

    r = _requests_utils.requests_get(url=f"{base_url}/catalog/datasets/{dataset_id}")
    r.raise_for_status()

    data_fields = r.json().get("fields")
    if data_fields is None:
        logging.error(f"No 'fields' found in dataset {dataset_id} response.")
        return None, None, additional_information

    datetime_columns = [col for col in data_fields if col.get('type') == 'datetime']
    date_columns = [col for col in data_fields if col.get('type') == 'date']

    additional_information["date_fields_found"] = [col['name'] for col in datetime_columns + date_columns]

    if datetime_columns:
        relevant_column_names = [col['name'] for col in datetime_columns]
        additional_information["granularity_used"] = "datetime"
    elif date_columns:
        relevant_column_names = [col['name'] for col in date_columns if col.get('annotations', {}).get('timeserie_precision', '') == 'day']
        additional_information["granularity_used"] = "day"

        if not relevant_column_names:
            relevant_column_names = [col['name'] for col in date_columns if col.get('annotations', {}).get('timeserie_precision', '') == 'month']
            additional_information["granularity_used"] = "month"

        if not relevant_column_names:
            relevant_column_names = [col['name'] for col in date_columns if col.get('annotations', {}).get('timeserie_precision', '') == 'year']
            additional_information["granularity_used"] = "year"

        if not relevant_column_names:
            logging.warning(f"No suitable date fields found for dataset {dataset_id}")
            return None, None, additional_information

    else:
        return None, None, additional_information

    additional_information["date_fields_considered"] = relevant_column_names

    min_return_value = None
    max_return_value = None
    for column_name in relevant_column_names:
        link_for_min = f"{base_url}/catalog/datasets/{dataset_id}/records?select={column_name}&order_by={column_name}%20ASC&limit=1"
        link_for_max = f"{base_url}/catalog/datasets/{dataset_id}/records?select={column_name}&order_by={column_name}%20DESC&limit=1"
        r_min = _requests_utils.requests_get(url=link_for_min)
        r_min.raise_for_status()

        r_max = _requests_utils.requests_get(url=link_for_max)
        r_max.raise_for_status()

        try:
            min_date = r_min.json().get('results', {})[0][column_name]
            max_date = r_max.json().get('results', {})[0][column_name]
        except IndexError:
            logging.error(f"Insufficient results returned for column {column_name} in dataset {dataset_id}.")
            continue

        if min_date is None or max_date is None:
            logging.warning(f"Skipping column {column_name} due to missing date value.")
            continue

        try:
            min_date_candidate = _parse_date(min_date, is_min_date=True)
            max_date_candidate = _parse_date(max_date, is_min_date=False)
        except ValueError as e:
            logging.error(f"Date parsing error for column {column_name} in dataset {dataset_id}: {e}")
            continue

        if min_return_value is None or min_date_candidate < min_return_value:
            logging.debug(f"Found oldest date {min_date_candidate}")
            min_return_value = min_date_candidate

        if max_return_value is None or max_date_candidate > max_return_value:
            logging.debug(f"Found newest date {max_date_candidate}")
            max_return_value = max_date_candidate

    min_date_str = None
    max_date_str = None
    if min_return_value and max_return_value:
        min_date_str = min_return_value.strftime('%Y-%m-%d')
        max_date_str = max_return_value.strftime('%Y-%m-%d')
        additional_information["status"] = "Success"
    else:
        additional_information["status"] = "Error: Unable to determine date range"

    return min_date_str, max_date_str, additional_information


def main():
    # Create an empty DataFrame to store the data for the CSV
    df = pd.DataFrame(columns=[
        'dataset_id',
        'dataset_title',
        'date_fields_found',
        'date_fields_considered',
        'granularity_used',
        'min_date',
        'max_date',
        'status'
    ])

    all_dataset_ids: [str] = ods_utils.get_all_dataset_ids(include_restricted=False)

    for counter, dataset_id in enumerate(all_dataset_ids, start=1):
            
        logging.info(f"Processing dataset [{counter}/{len(all_dataset_ids)}]: {dataset_id}")
        dataset_title = ods_utils.get_dataset_title(dataset_id=dataset_id)

        logging.info(f"Trying to retrieve oldest and newest date in the dataset {dataset_id}")
        try:
            min_date, max_date, additional_info = get_dataset_date_range(dataset_id=dataset_id)
        except requests.exceptions.HTTPError as e:
            logging.debug(f"HTTPError occurred: {e}")
            if e.response.status_code == 404:
                logging.info(f"Dataset {dataset_id} does not seem to exist. Skipping...")
            continue

        logging.info(f"Found dates in dataset {dataset_id} from {min_date} to {max_date}")

        new_row = pd.DataFrame({
            'dataset_id': [dataset_id],
            'dataset_title': [dataset_title],
            'date_fields_found': [', '.join(additional_info["date_fields_found"])],
            'date_fields_considered': [', '.join(additional_info["date_fields_considered"])],
            'granularity_used': [additional_info["granularity_used"]],
            'min_date': [min_date],
            'max_date': [max_date],
            'status': [additional_info["status"]]
        })

        df = pd.concat([df, new_row], ignore_index=True)

        # IMPORTANT: For checking whether a date has changed, we use the date that is written into the "temporal period"
        # field. When this field is not available, used, or updated anymore, we have to change how this works!

        currently_set_dates = ods_utils.get_dataset_metadata_temporal_period(dataset_id=dataset_id)
        if not currently_set_dates or '/' not in currently_set_dates:
            min_return_value = None
            max_return_value = None
        else:
            min_return_value = _parse_date(currently_set_dates.split('/')[0], is_min_date=True)
            max_return_value = _parse_date(currently_set_dates.split('/')[1], is_min_date=False)

        if min_date and max_date:
            should_update_min_date = min_return_value != _parse_date(min_date, is_min_date=True)
            should_update_max_date = max_return_value != _parse_date(max_date, is_min_date=False)

            if should_update_min_date:
                if min_return_value:
                    logging.info(f"Temporal coverage start date gets updated from {min_return_value.strftime('%Y-%m-%d')} to {min_date}")
                else:
                    logging.info(f"Temporal coverage start date gets updated from None to {min_date}")
            else:
                logging.info(f"Temporal coverage start date is {min_date} and does NOT need to be updated.")

            if should_update_max_date:
                if max_return_value:
                    logging.info(f"Temporal coverage end date gets updated from {max_return_value.strftime('%Y-%m-%d')} to {max_date}")
                else:
                    logging.info(f"Temporal coverage end date gets updated from None to {max_date}")
            else:
                logging.info(f"Temporal coverage end date is {max_date} and does NOT need to be updated.")

            # ISO 8601 standard for date ranges is "YYYY-MM-DD/YYYY-MM-DD"; we implement this here
            if should_update_min_date or should_update_max_date:
                ods_utils.set_dataset_metadata_temporal_period(
                    temporal_period=f"{min_date}/{max_date}",
                    dataset_id=dataset_id,
                    publish=False
                )
                logging.info(f"Update temporal period from {currently_set_dates} to {min_date}/{max_date}")

            if should_update_min_date:
                ods_utils.set_dataset_metadata_temporal_coverage_start_date(
                    temporal_coverage_start_date=min_date,
                    dataset_id=dataset_id,
                    publish=False
                )

            if should_update_max_date:
                ods_utils.set_dataset_metadata_temporal_coverage_end_date(
                    temporal_coverage_end_date=max_date,
                    dataset_id=dataset_id,
                    publish=False
                )

            if should_update_min_date or should_update_max_date:
                ods_utils.set_dataset_public(dataset_id=dataset_id, should_be_public=True)

        else:
            logging.warning(f"Skipping metadata update for dataset {dataset_id} due to missing date range.")

        logging.info(f"Dataset {dataset_id} process finished")

    # Save the DataFrame to a CSV file
    csv_filename = 'update_temporal_coverage_report.csv'
    csv_path = os.path.join('stata_ods', 'daily_jobs', 'update_temporal_coverage', csv_filename)
    df.to_csv(csv_path, index=False, sep=';')
    logging.info(f"CSV file '{csv_filename}' has been created with the dataset information. It has been saved to {csv_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
