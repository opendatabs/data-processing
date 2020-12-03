from md_covid19cases import credentials
import common
import os
import pandas as pd

print(f'Reading data into dataframe...')
filename = os.path.join(credentials.path, credentials.filename_faelle)
df = pd.read_csv(filename, sep=';')
print(f'Keeping only certain columns...')
df = df[['datum', 'publikationsdatum', 'faelle_bs_kum', 'faelle_bs', 'inzidenz07_bs', 'inzidenz14_bs', 'summe_07_tage', 'summe_14_tage', 'mittel_07_tage', 'mittel_14_tage']]

export_filename = os.path.join(credentials.export_path, credentials.export_filename_faelle)
print(f'Exporting csv to {export_filename}')
df.to_csv(export_filename, index=False)

common.upload_ftp(export_filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'md/covid19_cases')
print('Job successful!')
