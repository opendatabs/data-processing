import logging
import sys
import time

import common

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    ods_dataset_ids = sys.argv[1].split(",")
    count = len(ods_dataset_ids)
    print("Publishing ODS datasets...")
    for i, dataset_id in enumerate(ods_dataset_ids):
        common.publish_ods_dataset_by_id(dataset_id)
        if i < (count - 1):
            print("Waiting 5 seconds before sending next publish request to ODS...")
            time.sleep(5)
    print("Job successful!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f"Executing {__file__}...")
    main()
