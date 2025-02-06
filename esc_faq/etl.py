import os
import logging
import pandas as pd
import markdown
from markdown_newtab import NewTabExtension

import common
from esc_faq import credentials


def main():
    # Iterate over every excel
    df_all = pd.DataFrame()
    for filename in os.listdir(credentials.data_orig_path):
        if not filename.endswith('.xlsx'):
            logging.info(f"Ignoring {filename}; Not an excel file")
            continue

        excel_file_path = os.path.join(credentials.data_orig_path, filename)
        df = pd.read_excel(excel_file_path, usecols='A:J', engine='openpyxl')
        df.columns = ['Ranking', 'Frage', 'Antwort', 'Sprache', 'Verantwortung', 'Kontakt', 'Link',
                      'Zuletzt aktualisiert', 'Thema', 'Keywords']

        logging.info(f"Processing {filename} with {df.shape} rows. Turning markdown into HTML...")
        # Turn markdown into HTML
        # nl2br: Newlines are turned into <br> tags
        # NewTabExtension: Links open in a new tab by adding target="_blank"
        df['Antwort HTML'] = df['Antwort'].apply(
            lambda x: markdown.markdown(x, extensions=['nl2br', NewTabExtension()]) if pd.notna(x) else x)
        df_all = pd.concat([df_all, df])

    path_export = os.path.join(credentials.data_path, '100417_esc_faq.csv')
    df_all.to_csv(path_export, index=False)
    common.update_ftp_and_odsp(path_export=path_export,
                               folder_name="aussenbez-marketing/esc_faq",
                               dataset_id="100417")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info(f'Job completed successfully!')
