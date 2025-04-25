import glob

import pandas as pd

# Import data from http://parkendd.de/dumps/Basel.tar.xz and untar it to this directory:
import_path = "C:\\dev\\workspace\\data-processing\\parkendd\\historical_data"
print(f"Searching csv files in {import_path}...")
all_files = glob.glob(import_path + "/*.csv")

all_all = []
all_hourly = []

for filename in all_files:
    print(f"Processing {filename}...")
    id = filename.split("\\")[-1][:-9]
    id2 = id[13:]
    df = pd.read_csv(filename, index_col=None, names=["published", "free"])
    df["id"] = id
    df["id2"] = id2
    df["published_dt"] = pd.to_datetime(df["published"])
    df.set_index("published_dt", drop=False, inplace=True)
    all_all.append(df)

    # slow way of getting first value of each hour:
    # df['date_hour'] = df.published.apply(lambda x: x.strftime('%Y-%m-%dT%H'))
    # hourly = df.groupby(['date_hour']).apply(lambda x: x.iloc[[0]])
    # hourly.drop(columns=['date_hour'], inplace=True)
    # all_hourly.append(hourly)

    # fast way of getting first value of each hour:
    print("Keping only first value of every hour...")
    hourly = df.reset_index(drop=True)
    hourly.set_index(["published_dt"], drop=False, inplace=True)
    hourly = df.resample("H", on="published_dt", convention="start").first()
    hourly.drop(columns=["published_dt"], inplace=True)
    hourly.dropna(inplace=True)
    all_hourly.append(hourly)

print("Concatenating all dataframes...")
all_df = pd.concat(all_all, axis=0, ignore_index=True)
all_hourly_df = pd.concat(all_hourly, axis=0, ignore_index=True)
all_hourly_df["free"] = all_hourly_df["free"].astype(int)

print("Exporting csv files...")
export_path = "C:/dev/workspace/data-processing/parkendd/data/csv/"
all_df.to_csv(f"{export_path}history_complete.csv", index=False)
all_hourly_df.to_csv(f"{export_path}history_hourly.csv", index=False)

# Better upload files manually.
# print('csv file to ftp server...')
# common.upload_ftp(csv_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parkendd/csv')
# common.upload_ftp(json_file_name, credentials.ftp_server, credentials.ftp_user, credentials.ftp_pass, 'parkendd/json')

print("Job successful!")
