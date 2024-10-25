"""
This class updates the temporal coverage fields of all datasets by automatically detecting date or datetime columns. It
sorts these columns by granularity and only considers those with the highest granularity available. Specifically:

- If datetime fields are present, only these are considered.
- If no datetime fields are found, only date fields with 'day' granularity are used.
- If no fields with 'day' granularity are available, fields with 'month' granularity are used.
- If no 'month' fields are present, fields with 'year' granularity are used.

If none of these granularities are present, the process skips the dataset and moves to the next.
"""

# TODO: Create a csv with the following header: dataset_id,dataset_title,date_fields_found,fields_considered,fields_skipped,granularity_used,min_date,max_date,status
import logging
from typing import Optional
from dateutil.relativedelta import relativedelta

import ods_utils_py as ods_utils
from ods_utils_py import _requests_utils

from datetime import datetime

def _parse_date(date: str, is_min_date: bool) -> Optional[datetime]:
    """
    Creates a datetime object in canonical form. Does not consider time, and always rounds the date to day-precision.
    When given only the year, e.g. 2020, this will result in 01.01.2020 (min_date) or 31.12.2020 (max_date),
    When given only the month, e.g. 2016-05, this will result in 01.05.2016 (min_date) or 31.05.2016 (max_date).

    Args:
        date: The date to parse in the form %Y-%m-%d, %Y-%m or %Y. Can also handle more granular dates, for example
            the date 2018-06-01T00:00:00+00:00 will be handled like 2018-06-01
        is_min_date: If is_min_date is False, then it is a max_date. This is relevant for rounding, e.g. whether 2024-03
            gets rounded to 2024-03-01 (min_date) or 2024-03-31 (max_date)

    Returns: A datetime object
    """

    date_no_time = date.split('T')[0]

    match len(date.split('-')):
        case 3: # Day precision
            dt = datetime.strptime(date_no_time, '%Y-%m-%d')
            return dt

        case 2: # Month precision
            dt = datetime.strptime(date_no_time, '%Y-%m')
            if not is_min_date:
                dt = dt + relativedelta(months=1) - relativedelta(days=1)
            return dt

        case 1: # Year precision
            dt = datetime.strptime(date_no_time, '%Y')
            if not is_min_date:
                dt = dt + relativedelta(years=1) - relativedelta(days=1)
            return dt

        case _:
            ValueError(f"Date format for '{date}' not recognized")
            return None

def get_dataset_date_range(dataset_id: str) -> (str, str):
    """
    Find the oldest and newest date in the dataset. This will only consider columns that are of the format datetime or
        date

    Args:
        dataset_id: The id of the dataset that can be seen in the url of the dataset

    Returns: A tuple of the oldest and newest date in the dataset, both formatted as YYYY-MM-DD, or (None, None) if no
        fitting date columns are found.
    """

    base_url = "https://data.bs.ch/api/explore/v2.1"

    r = _requests_utils.requests_get(url=f"{base_url}/catalog/datasets/{dataset_id}")
    r.raise_for_status()

    data_fields = r.json().get("fields")

    datetime_columns = [col for col in data_fields if col.get('type') == 'datetime']
    date_columns = [col for col in data_fields if col.get('type') == 'date']

    if datetime_columns:
        relevant_column_names = [col['name'] for col in datetime_columns]

    elif date_columns:
        relevant_column_names = [col['name'] for col in date_columns if col.get('annotations', {}).get('timeserie_precision', '') == 'day']

        if not relevant_column_names:
            relevant_column_names = [col['name'] for col in date_columns if col.get('annotations', {}).get('timeserie_precision', '') == 'month']

        if not relevant_column_names:
            relevant_column_names = [col['name'] for col in date_columns if col.get('annotations', {}).get('timeserie_precision', '') == 'year']

        if not relevant_column_names:
            logging.warning(f"Precision of column {relevant_column_names[0][0]} does not seem to be 'day', 'month' or 'year'")
            relevant_column_names = []

    else:
        return None, None

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
            return None, None



        min_date_candidate = _parse_date(min_date, is_min_date=True)
        max_date_candidate = _parse_date(max_date, is_min_date=False)

        if min_return_value is None or min_date_candidate < min_return_value:
            logging.debug(f"Updating min_return_value from {min_return_value} to {min_date_candidate}")
            min_return_value = min_date_candidate

        if max_return_value is None or max_date_candidate > max_return_value:
            logging.debug(f"Updating max_return_value from {max_return_value} to {max_date_candidate}")
            max_return_value = max_date_candidate

    min_date_str = None
    max_date_str = None
    if min_return_value and max_return_value:
        min_date_str = min_return_value.strftime('%Y-%m-%d')
        max_date_str = max_return_value.strftime('%Y-%m-%d')

    return min_date_str, max_date_str

def main():
    #all_dataset_ids: [int] = ods_utils.get_all_dataset_ids()

    for dataset_id in ['100397']:#all_dataset_ids:
        logging.info(f"Processing dataset {dataset_id}")

        logging.info(f"Trying to retrieve oldest and newest date in the dataset {dataset_id}")
        date_range_from, date_range_to = get_dataset_date_range(dataset_id=dataset_id)
        logging.info(f"Found dates in dataset {dataset_id} from {date_range_from} to {date_range_to}")

        # ISO 8601 standard for date ranges is "YYYY-MM-DD/YYYY-MM-DD"; we implement this here
        ods_utils.set_dataset_metadata_temporal_period(temporal_period=f"{date_range_from}/{date_range_to}",
                                                       dataset_id=dataset_id, publish=False)

        ods_utils.set_dataset_metadata_temporal_coverage_start_date(temporal_coverage_start_date=date_range_from,
                                                                    dataset_id=dataset_id, publish=False)
        ods_utils.set_dataset_metadata_temporal_coverage_end_date(temporal_coverage_end_date=date_range_to,
                                                                  dataset_id=dataset_id, publish=True)

        logging.info(f"Dataset {dataset_id} process finished")
    pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
