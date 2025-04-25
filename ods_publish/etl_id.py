import logging
import sys
import time

import common
from common import ODS_API_KEY

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    ods_dataset_ids = sys.argv[1].split(",")
    count = len(ods_dataset_ids)
    print("Publishing ODS datasets...")
    for i, dataset_id in enumerate(ods_dataset_ids):
        publish_ods_dataset_by_id(dataset_id)
        if i < (count - 1):
            print("Waiting 5 seconds before sending next publish request to ODS...")
            time.sleep(5)
    print("Job successful!")


def publish_ods_dataset_by_id(dataset_id: str, unpublish_first=False):
    dataset_uid = common.get_ods_uid_by_id(dataset_id)
    common.publish_ods_dataset(dataset_uid, unpublish_first=unpublish_first)


def unpublish_ods_dataset_by_id(dataset_id: str):
    dataset_uid = common.get_ods_uid_by_id(dataset_id)
    common.unpublish_ods_dataset(dataset_uid)


def ods_set_general_access_policy(
    dataset_id: str, access_should_be_restricted: bool, do_publish=True
):
    dataset_uid = common.get_ods_uid_by_id(dataset_id)
    logging.info("Getting General Access Policy before setting it...")
    url = f"https://data.bs.ch/api/automation/v1.0/datasets/{dataset_uid}/"
    r = common.requests_get(url=url, headers={"Authorization": f"apikey {ODS_API_KEY}"})
    r.raise_for_status()
    is_currently_restricted = r.json()["is_restricted"]
    do_change_policy = is_currently_restricted != access_should_be_restricted
    logging.info(
        f"Current access policy: {is_currently_restricted}. Do we have to change that? {do_change_policy}."
    )
    if do_change_policy:
        logging.info(
            f"Setting General Access Policy to is_restricted={access_should_be_restricted}..."
        )
        r = common.requests_put(
            url=url,
            data={"is_restricted": access_should_be_restricted},
            headers={"Authorization": f"apikey {ODS_API_KEY}"},
        )
        r.raise_for_status()
        if do_publish:
            logging.info("Publishing dataset...")
            common.publish_ods_dataset(dataset_uid)
    return do_change_policy, r


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
