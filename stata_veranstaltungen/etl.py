import common
from stata_veranstaltungen import credentials

filename = 'veranstaltungen.csv'
common.upload_ftp(credentials.path_work + filename, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'veranstaltungen')

# print('Publishing ODS dataset...')
# common.publish_ods_dataset('da_so5l56', credentials)

print('Job successful!')