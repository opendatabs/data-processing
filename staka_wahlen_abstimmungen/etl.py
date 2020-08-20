from staka_wahlen_abstimmungen import credentials
import pandas as pd
import os
import common

import_file_name = os.path.join(credentials.path, credentials.data_orig)
print(f'Reading dataset from {import_file_name}...')
df = pd.read_excel(import_file_name, sheet_name='DAT 1', skiprows=4, header=[0,1,2], index_col=None)
df.reset_index

print(df.columns)

# export_file_name = os.path.join(credentials.path, credentials.data_export)
# print(f'Exporting to {export_file_name}...')
# df_nototal.to_csv(export_file_name, index=False)

# common.upload_ftp(export_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'wahlen_abstimmungen')
print('Job successful!')

