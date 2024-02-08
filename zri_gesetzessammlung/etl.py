import logging
import os
import json
import pandas as pd

import common
from zri_gesetzessammlung import credentials


def main():
    r = common.requests_get('http://lexfind/api/fe/de/global/systematics')
    r.raise_for_status()
    stats = r.json()
    with open(os.path.join(credentials.data_path, 'systematics_global.json'), 'w') as f:
        json.dump(stats, f, indent=2)
    quit()


    # Systematics of BS
    r = common.requests_get('http://lexfind/api/fe/de/entities/6/systematics')
    r.raise_for_status()
    systematics = r.json()
    with open(os.path.join(credentials.data_path, 'systematics_BS.json'), 'w') as f:
        json.dump(systematics, f, indent=2)
    df_sys = pd.DataFrame(systematics).T.reset_index()
    df_sys.to_csv(os.path.join(credentials.data_path, 'systematics_BS.csv'), index=False)

    # Recent changes of BS
    r = common.requests_get('http://lexfind/api/fe/de/entities/6/recent-changes')
    r.raise_for_status()
    recent_changes = r.json()
    with open(os.path.join(credentials.data_path, 'recent_changes_BS.json'), 'w') as f:
        json.dump(recent_changes, f, indent=2)
    df_rc = pd.json_normalize(recent_changes, record_path='recent_changes')

    for _ in range(3):
        domain_suffix = recent_changes['next_batch']
        r = common.requests_get('http://lexfind' + domain_suffix)
        r.raise_for_status()
        recent_changes = r.json()
        df_rc = pd.concat((df_rc, pd.json_normalize(recent_changes, record_path='recent_changes')))

    df_rc.to_csv(os.path.join(credentials.data_path, 'recent_changes_BS.csv'), index=False)





if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
    logging.info("Job successful!")
