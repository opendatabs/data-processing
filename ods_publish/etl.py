import sys
import common
from ods_publish import credentials
import time

import sys
ods_dataset_uids = sys.argv[1].split(',')

print('Publishing ODS datasets...')
for datasetuid in ods_dataset_uids:
    common.publish_ods_dataset(datasetuid, credentials)
    print('Waiting 5 seconds before sending next publish request to ODS...')
    time.sleep(5)

print('Job successful!')