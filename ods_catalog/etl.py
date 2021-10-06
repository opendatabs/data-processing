import common
import os
from common import change_tracking as ct
import ods_publish.etl_id as odsp
from ods_catalog import credentials
import logging


def main():
    url = 'https://data.bs.ch/explore/dataset/100055/download/?format=csv&use_labels_for_header=true&refine.visibility=domain&refine.publishing_published=True'
    file = os.path.join(credentials.path, credentials.filename)
    print(f'Downloading {file} from {url}...')
    r = common.requests_get(url, auth=(credentials.ods_user, credentials.ods_password))
    f = open(file, 'wb')
    f.write(r.content)
    f.close()
    if ct.has_changed(file):
        common.upload_ftp(filename=file, server=credentials.ftp_server, user=credentials.ftp_user, password=credentials.ftp_pass, remote_path=credentials.ftp_path)
        odsp.publish_ods_dataset_by_id('100057')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
