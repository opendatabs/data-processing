# import pandas as pd
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
    if ct.has_changed(file, do_update_hash_file=False):
        # df = pd.read_csv(file, sep=';')
        # {
        #     "Dataset identifier": 100071,
        #     "Federated dataset": false,
        #     "Title": "Abstimmung 27. September 2020 Details",
        #     "Description": "<p>Text<\/p>",
        #     "Themes": "Politik",
        #     "Keywords": "Wahlen;Abstimmung;Demokratie;Teilhabe",
        #     "License": "CC BY 3.0 CH",
        #     "Language": "de",
        #     "Timezone": null,
        #     "Modified": "2020-09-27T13:28:26.329000+00:00",
        #     "Data processed": "2020-09-27T13:28:26.329000+00:00",
        #     "Metadata processed": "2020-11-29T15:24:43.566000+00:00",
        #     "Publisher": "Staatskanzlei",
        #     "Reference": "http:\/\/abstimmungen.bs.ch",
        #     "Attributions": null,
        #     "Created": "2020-09-10",
        #     "Issued": "2020-09-27",
        #     "Creator": "Staatskanzlei des Kantons Basel-Stadt",
        #     "Contributor": null,
        #     "Contact name": "Fachstelle f\u00fcr OGD Basel-Stadt",
        #     "Contact email": "opendata@bs.ch",
        #     "Accrual periodicity": null,
        #     "Spatial": null,
        #     "Temporal": null,
        #     "Granularity": null,
        #     "Data quality": null,
        #     "Publisher type": null,
        #     "Conforms to": null,
        #     "Temporal coverage start date": null,
        #     "Temporal coverage end date": null,
        #     "Domain": null,
        #     "Rights": "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
        #     "RML Mapping": null,
        #     "Publizierende Organisation": "Staatskanzlei",
        #     "Geodaten Modellbeschreibung": null,
        #     "Tags": null,
        #     "Number of records": 50,
        #     "Size of records in the dataset (in bytes)": 9686,
        #     "Reuse count": 0,
        #     "API call count": 7173,
        #     "Download count": 16,
        #     "Attachments download count": 0,
        #     "File fields download count": 0,
        #     "Popularity score": 2.9,
        #     "Visibility (domain or restricted)": "domain",
        #     "Published": true,
        #     "Publishing properties": null
        # }
        # common.ods_realtime_push_df(df=df, url='', push_key='')
        common.upload_ftp(filename=file, server=credentials.ftp_server, user=credentials.ftp_user, password=credentials.ftp_pass, remote_path=credentials.ftp_path)
        odsp.publish_ods_dataset_by_id('100057')
        ct.update_hash_file(file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
