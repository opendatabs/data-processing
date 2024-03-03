import common
from ods_publish import credentials
import time
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)


def main():
    ods_dataset_ids = sys.argv[1].split(',')
    count = len(ods_dataset_ids)
    print('Publishing ODS datasets...')
    for i, dataset_id in enumerate(ods_dataset_ids):
        publish_ods_dataset_by_id(dataset_id)
        if i < (count - 1):
            print('Waiting 5 seconds before sending next publish request to ODS...')
            time.sleep(5)
    print('Job successful!')


def publish_ods_dataset_by_id(dataset_id: str, unpublish_first=False):
    dataset_uid = common.get_ods_uid_by_id(dataset_id, credentials)
    common.publish_ods_dataset(dataset_uid, credentials, unpublish_first=unpublish_first)


def unpublish_ods_dataset_by_id(dataset_id: str):
    dataset_uid = common.get_ods_uid_by_id(dataset_id, credentials)
    common.unpublish_ods_dataset(dataset_uid, credentials)


def ods_set_general_access_policy(dataset_id: str, access_policy: str, do_publish=True):
    possible_values = ['domain', 'restricted']
    if access_policy not in possible_values:
        raise NotImplementedError(f'You can only use policies {possible_values}.')
    dataset_uid = common.get_ods_uid_by_id(dataset_id, credentials)
    logging.info(f'Getting General Access Policy before setting it...')
    url = f'https://data.bs.ch/api/management/v2/datasets/{dataset_uid}/security/access_policy'
    r = common.requests_get(url=url, headers={'Authorization': f'apikey {credentials.api_key}'})
    r.raise_for_status()
    existing_policy = r.text
    data = f'"{access_policy}"'
    do_change_policy = existing_policy != data
    logging.info(f'Current access policy: {existing_policy}. Do we have to change that? {do_change_policy}. ')
    if do_change_policy:
        logging.info(f'Setting General Access Policy to {data}...')
        r = common.requests_put(url=url, data=data, headers={'Authorization': f'apikey {credentials.api_key}'})
        r.raise_for_status()
        if do_publish:
            logging.info(f'Publishing dataset...')
            common.publish_ods_dataset(dataset_uid, credentials)
    return do_change_policy, r


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
