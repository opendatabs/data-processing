import common
import os
from ods_catalog import credentials
url = 'https://data.bs.ch/explore/dataset/100055/download/?format=csv&use_labels_for_header=true&refine.visibility=domain&refine.publishing_published=True'
file = os.path.join(credentials.path, credentials.filename)
print(f'Downloading {file} from {url}...')
r = common.requests_get(url, auth=(credentials.ods_user, credentials.ods_password))
open(file, 'wb').write(r.content)
common.upload_ftp(filename=file, server=credentials.ftp_server, user=credentials.ftp_user, password=credentials.ftp_pass, remote_path=credentials.ftp_path)
