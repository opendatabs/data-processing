import common
from ods_publish import credentials
import time
import logging
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)

ods_dataset_ids = sys.argv[1].split(',')
count = len(ods_dataset_ids)
print('Publishing ODS datasets...')
for i, dataset_id in enumerate(ods_dataset_ids):
    print(f'Retrieving dataset uid for id {dataset_id} from ODS...')
    dataset_uid = common.get_ods_uid_by_id(dataset_id, credentials)
    print(f'Received dataset uid "{dataset_uid}"...')
    common.publish_ods_dataset(dataset_uid, credentials)
    if i < (count-1):
        print('Waiting 5 seconds before sending next publish request to ODS...')
        time.sleep(5)

print('Job successful!')