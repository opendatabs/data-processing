import sys
import common
from ods_publish import credentials
import time

import sys
ods_dataset_uids = sys.argv[1].split(',')
count = len(ods_dataset_uids)
print('Publishing ODS datasets...')
for i, dataset_uid in enumerate(ods_dataset_uids):
    common.publish_ods_dataset(dataset_uid, credentials)
    if i < (count-1):
        print('Waiting 30 seconds before sending next publish request to ODS...')
        time.sleep(30)

print('Job successful!')