import logging
import os
import pathlib
import ods_publish.etl_id as odsp
import icalendar
import pandas as pd
import common
import locale
from tba_abfuhrdaten import credentials


def main():
    urls = ['/dam/jcr:2c9acc77-cd90-4530-9393-e29237d51f74/Zone%20A.ics', '/dam/jcr:cf5efdb2-284a-4a13-8002-19d0e8686d79/Zone%20B.ics', '/dam/jcr:4acae36e-9d24-48ed-8ca5-ab096a9f1889/Zone%20C.ics', '/dam/jcr:225425c1-dce8-423d-a048-5658a4dbd871/Zone%20D.ics', '/dam/jcr:e1b985d5-6085-4bee-b449-3fe1bd74c8e3/Zone%20E.ics', '/dam/jcr:32121b51-56db-4879-a229-33c36a507a6d/Zone%20F.ics', '/dam/jcr:f5e7fc35-b11d-46b2-a0b5-8d2d3d5178c4/Zone%20G.ics', '/dam/jcr:303e7222-dc48-4c09-8b36-54c0d5dbf38e/Zone%20H.ics']
    base_url = 'https://www.tiefbauamt.bs.ch'
    dfs = []
    for url in urls:
        r = common.requests_get(url=base_url+url, allow_redirects=True)
        r.raise_for_status()
        calendar = icalendar.Calendar.from_ical(r.content)
        locale.setlocale(locale.LC_TIME, "de_CH")
        df_cal = pd.DataFrame(dict(art=event['SUMMARY'], zone=event['location'].split(' ')[1], wochentag=event['DTSTART'].dt.strftime('%a'), termin=event['DTSTART'].dt.strftime('%d.%m.%Y')) for event in calendar.walk('VEVENT'))
        dfs.append(df_cal)
    all_df = pd.concat(dfs)
    export_file = os.path.join(pathlib.Path(__file__).parent, 'data', 'Abfuhrtermine_2023.csv')
    all_df.to_csv(export_file, sep=';', encoding='cp1252', index=False)
    common.upload_ftp(export_file, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'tba/abfuhrtermine')
    odsp.publish_ods_dataset_by_id('100096')
    pass


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
