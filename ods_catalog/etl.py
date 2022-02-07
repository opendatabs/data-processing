from io import StringIO
import common
from ods_catalog import credentials
import logging


def main():
    url = 'https://data.bs.ch/explore/dataset/100055/download/?format=csv&use_labels_for_header=true&refine.visibility=domain&refine.publishing_published=True'
    r = common.requests_get(url, auth=(credentials.ods_user, credentials.ods_password))
    df = common.pandas_read_csv(StringIO(r.text), sep=';')
    common.ods_realtime_push_df(df=df, url=credentials.ods_push_url, push_key=credentials.ods_push_key)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()


# ODS realtime push bootstrap:
# {
#     "Dataset identifier": 100071,
#     "Federated dataset": "Text",
#     "Title": "Abstimmung 27. September 2020 Details",
#     "Description": "<p>Text<\/p>",
#     "Themes": "Politik",
#     "Keywords": "Wahlen;Abstimmung;Demokratie;Teilhabe",
#     "License": "CC BY 3.0 CH",
#     "Language": "de",
#     "Timezone": "Text",
#     "Modified": "2020-09-27T13:28:26.329000+00:00",
#     "Data processed": "2020-09-27T13:28:26.329000+00:00",
#     "Metadata processed": "2020-11-29T15:24:43.566000+00:00",
#     "Publisher": "Staatskanzlei",
#     "Reference": "http:\/\/abstimmungen.bs.ch",
#     "Attributions": "Text",
#     "Created": "2020-09-10",
#     "Issued": "2020-09-27",
#     "Creator": "Staatskanzlei des Kantons Basel-Stadt",
#     "Contributor": "Text",
#     "Contact name": "Fachstelle f\u00fcr OGD Basel-Stadt",
#     "Contact email": "opendata@bs.ch",
#     "Accrual periodicity": "Text",
#     "Spatial": "Text",
#     "Temporal": "2020-09-10",
#     "Granularity": "Text",
#     "Data quality": "Text",
#     "Publisher type": "Text",
#     "Conforms to": "Text",
#     "Temporal coverage start date": "2020-09-10",
#     "Temporal coverage end date": "2020-09-10",
#     "Domain": "Text",
#     "Rights": "NonCommercialAllowed-CommercialAllowed-ReferenceRequired",
#     "RML Mapping": "Text",
#     "Publizierende Organisation": "Staatskanzlei",
#     "Geodaten Modellbeschreibung": "Text",
#     "Tags": "Text",
#     "Number of records": 50,
#     "Size of records in the dataset (in bytes)": 9686,
#     "Reuse count": 0,
#     "API call count": 7173,
#     "Download count": 16,
#     "Attachments download count": 0,
#     "File fields download count": 0,
#     "Popularity score": 2.9,
#     "Visibility (domain or restricted)": "domain",
#     "Published": "Text",
#     "Publishing properties": "Text"
# }
