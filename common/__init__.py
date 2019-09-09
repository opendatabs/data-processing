from ftplib import FTP
import os

# Upload file to FTP Server
def upload_ftp(filename, server, user, password, remote_path):
    # change to desired directory first
    currdir = os.getcwd()
    relpath, filename_nopath = os.path.split(filename)
    if relpath != '':
        os.chdir(relpath)
    ftp = FTP(server)
    ftp.login(user, password)
    ftp.cwd(remote_path)
    print ("Uploading " + filename_nopath + " to FTP server directory " + remote_path + '...')
    with open(filename_nopath, 'rb') as f:
        ftp.storbinary('STOR %s' % filename_nopath, f)
    ftp.quit()
    os.chdir(currdir)
    return
