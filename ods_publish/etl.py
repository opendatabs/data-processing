import sys
import common
from ods_publish import credentials
import time

import sys
ods_dataset_uids = sys.argv[1].split(',')

print('Publishing ODS datasets...')
for datasetuid in ods_dataset_uids:
    common.publish_ods_dataset(datasetuid, credentials)
    print('Waiting 1 minute before sending next publish request to ODS...')
    time.sleep(60)

print('Job successful!')