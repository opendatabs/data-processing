import pandas as pd
from wahlen import credentials
import logging
import os

def main():
    # the file we want to change
    ods_ids = [100131, 100132, 100133]
    # create an explicit pattern (2024)
    df_new = pd.read_csv(credentials.template_file_path, delimiter="\t",encoding='ISO-8859-1', index_col=False) 
    df_new = pd.DataFrame(columns=df_new.columns)
    for Id in ods_ids:
        # read the old file to change it
        old_file_path = os.path.join(credentials.old_file_path,f'{Id}.xlsx')
        df_old = pd.read_excel(old_file_path)
        # find the common column names
        common_col = df_old.columns.intersection(df_new.columns)
        # replace the columns in new with the columns from old
        df_new[common_col] = df_old[common_col]
        df_new['Wahlbezeichnung'] = df_old['Titel']
        df_new['Wahltermin'] = df_old['Datum']
        df_new['Bezeichnung Wahlkreis'] = df_old['Gemeinde']
        df_new['Wahlzettel'] = df_old['Stimmrechtsausweise']
        # add a new column
        df_new['Wahlgang'] = df_old['Wahlgang']
        # reform the entries in columns 
        df_new['Gewählt'] = df_new['Gewählt'].replace({'ja':'Gewählt', 'nein':'Nicht gewählt'})
        df_new['Stimmbeteiligung'] = (df_new['Stimmbeteiligung'] * 100).round(2).astype(str) + '%'
        df_new['Anteil brieflich Wählende'] = (df_new['Anteil brieflich Wählende'] * 100).round(2).astype(str) + '%'
        new_file_path = os.path.join(credentials.new_file_path,f'{Id}.txt')
        df_new.to_csv(new_file_path, sep='\t', index=False)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    main()
    logging.info('Job successful!')
