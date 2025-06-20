import os
from datetime import datetime
from ftplib import FTP

import common
import numpy as np
import pandas as pd

from bafu_hydrodaten import credentials

print("Connecting to FTP Server to read data...")
ftp = FTP(credentials.ftp_read_server, credentials.ftp_read_user, credentials.ftp_read_pass)
print(f"Changing to remote dir {credentials.ftp_read_remote_path}...")
ftp.cwd(credentials.ftp_read_remote_path)
local_path = "bafu_hydrodaten/data"
files = [credentials.abfluss_file, credentials.pegel_file]
local_files = []
for file in files:
    local_file = os.path.join(local_path, file)
    local_files.append(local_file)
    print(f"Retrieving file {local_file}...")
    with open(local_file, "wb") as f:
        ftp.retrbinary(f"RETR {file}", f.write)
ftp.quit()

print("Loading data...")
abfluss_df = pd.read_csv(
    local_files[0],
    sep="\t",
    skiprows=4,
    names=["datum", "zeit", "abfluss", "intervall", "qualitaet", "messart"],
    usecols=["datum", "zeit", "abfluss", "intervall"],
    header=None,
)
pegel_df = pd.read_csv(
    local_files[1],
    sep="\t",
    skiprows=4,
    names=["datum", "zeit", "pegel", "intervall", "qualitaet", "messart"],
    usecols=["datum", "zeit", "pegel", "intervall"],
    header=None,
)

print("Merging data...")
merged_df = abfluss_df.merge(pegel_df, on=["datum", "zeit", "intervall"], how="outer")

print("Processing data...")
merged_df = merged_df.loc[merged_df.intervall == 5]
print("Fixing entries with zeit == 24:00...")
# Replacing 24:00 with 23:59
merged_df.loc[merged_df.zeit == "24:00", "zeit"] = "23:59"
# Time is given in MEZ (UTC+1) thus use 'Etc/GMT-1' according to https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
# merged_df['timestamp'] = pd.to_datetime(merged_df.datum + ' ' + merged_df.zeit, format='%d.%m.%Y %H:%M').dt.tz_localize('Europe/Zurich')
merged_df["timestamp"] = pd.to_datetime(merged_df.datum + " " + merged_df.zeit, format="%d.%m.%Y %H:%M").dt.tz_localize(
    "Etc/GMT-1"
)
# Adding a minute to entries with time 23:59 then replacing 23:59 with 24:00 again
merged_df.timestamp = np.where(
    merged_df.zeit != "23:59",
    merged_df.timestamp,
    merged_df.timestamp + pd.Timedelta(minutes=1),
)
merged_df.zeit = np.where(merged_df.zeit == "23:59", "24:00", merged_df.zeit)
merged_filename = os.path.join(
    local_path,
    f"2289_pegel_abfluss_{datetime.today().strftime('%Y-%m-%dT%H-%M-%S')}.csv",
)
merged_df.to_csv(merged_filename, index=False)

common.upload_ftp(
    merged_filename,
    credentials.ftp_server,
    credentials.ftp_user,
    credentials.ftp_pass,
    credentials.ftp_remote_dir,
)
print("Job successful!")
