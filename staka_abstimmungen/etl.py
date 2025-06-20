import glob
import logging
import os
import re
import smtplib
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import common
import common.change_tracking as ct
import pandas as pd
from common import EMAIL_RECEIVERS, EMAIL_SERVER
from dotenv import load_dotenv
from etl_details import calculate_details
from etl_kennzahlen import calculate_kennzahlen

load_dotenv()

ODS_PUSH_URL_DETAILS_TEST = os.getenv("ODS_PUSH_URL_100343")
ODS_PUSH_URL_KENNZ_TEST = os.getenv("ODS_PUSH_URL_100344")
ODS_PUSH_URL_DETAILS_PUBLIC = os.getenv("ODS_PUSH_URL_100345")
ODS_PUSH_URL_KENNZ_PUBLIC = os.getenv("ODS_PUSH_URL_100346")


def main():
    push_past_abstimmungen = False
    logging.info(f"Pushing abstimmungen from the past to ods? {push_past_abstimmungen}")
    if push_past_abstimmungen:
        push_past_abstimmungen_to_ods()
        return
    logging.info("Reading control.csv...")
    df = pd.read_csv(
        os.path.join("data", "control.csv"),
        sep=";",
        parse_dates=["Ignore_changes_before", "Embargo", "Ignore_changes_after"],
    )
    active_abst = df.query("Active == True").copy(deep=True)
    active_active_size = active_abst.Active.size
    what_changed = {"updated_ods_datasets": [], "send_update_email": False}
    if active_active_size == 1:
        abst_date = active_abst.Abstimmungs_datum[0]
        logging.info(f"Processing Abstimmung for date {abst_date}...")
        do_process, make_live_public = check_embargos(active_abst, active_active_size)
        logging.info(f"Should we check for changes in data files now? {do_process}")
        if do_process:
            active_files = find_data_files_for_active_abst(active_abst)
            data_files_changed = have_data_files_changed(active_files)
            logging.info(f"Have the data files changed? {data_files_changed}. ")
            logging.info(f"Is it time to make live datasets public? {make_live_public}. ")
            if data_files_changed or make_live_public:
                df_details, details_changed, df_kennz, kennz_changed = calculate_and_upload(active_files)
                common.ods_realtime_push_df(df_details, ODS_PUSH_URL_DETAILS_TEST)
                common.ods_realtime_push_df(df_kennz, ODS_PUSH_URL_KENNZ_TEST)
                what_changed = publish_datasets(details_changed, kennz_changed, what_changed=what_changed)
                for file in active_files:
                    ct.update_hash_file(os.path.join("data", file))

                if make_live_public:
                    what_changed = make_datasets_public(active_files, what_changed)
                    common.ods_realtime_push_df(df_details, ODS_PUSH_URL_DETAILS_PUBLIC)
                    common.ods_realtime_push_df(df_kennz, ODS_PUSH_URL_KENNZ_PUBLIC)
                if data_files_changed:
                    send_update_email(what_changed)

    elif active_active_size == 0:
        logging.info("No active Abstimmung, nothing to do for the moment. ")
    elif active_active_size > 1:
        raise NotImplementedError("Only one Abstimmung must be active at any time!")
    logging.info("Job Successful!")


def push_past_abstimmungen_to_ods():
    path_data_processing_output = os.path.join("data", "data-processing-output")
    files_details = glob.glob(os.path.join(path_data_processing_output, "Abstimmungen_Details_??????????.csv"))
    files_details = filter_files_by_date(files_details)
    files_kennz = glob.glob(os.path.join(path_data_processing_output, "Abstimmungen_??????????.csv"))
    files_kennz = filter_files_by_date(files_kennz)
    for file in files_details:
        df_details = pd.read_csv(file)
        common.ods_realtime_push_df(df_details, ODS_PUSH_URL_DETAILS_TEST)
        common.ods_realtime_push_df(df_details, ODS_PUSH_URL_DETAILS_PUBLIC)
    for file in files_kennz:
        df_kennz = pd.read_csv(file)
        common.ods_realtime_push_df(df_kennz, ODS_PUSH_URL_KENNZ_TEST)
        common.ods_realtime_push_df(df_kennz, ODS_PUSH_URL_KENNZ_PUBLIC)


def parse_date_from_filename(filename):
    # Extract date from filename assuming format 'YYYY-MM-DD'
    match = re.search(r"\d{4}-\d{2}-\d{2}", filename)
    if match:
        return datetime.strptime(match.group(), "%Y-%m-%d").date()
    return None


def filter_files_by_date(files):
    # Get today's date
    today = datetime.now().date()
    # Filter out files with a date in the future
    return [file for file in files if parse_date_from_filename(file) and parse_date_from_filename(file) <= today]


def send_update_email(what_changed):
    text = ""
    if len(what_changed["updated_ods_datasets"]) > 0:
        what_changed["send_update_email"] = True
        text += "Updated ODS Datasets: \n"
        for ods_id in what_changed["updated_ods_datasets"]:
            text += f"- {ods_id}: https://data.bs.ch/explore/dataset/{ods_id} \n"
    logging.info(f"Is it time to send an update email? {what_changed['send_update_email']}")
    if what_changed["send_update_email"]:
        text += "\n\nKind regards, \nYour automated Open Data Basel-Stadt Python Job"
        msg = common.email_message(
            subject="Abstimmungen: Updates have been automatically pushed to ODS",
            text=text,
        )
        host = EMAIL_SERVER
        smtp = smtplib.SMTP(host)
        smtp.sendmail(from_addr="opendata@bs.ch", to_addrs=EMAIL_RECEIVERS, msg=msg.as_string())
        smtp.quit()
        logging.info("Update email sent: ")
        logging.info(text)
    return what_changed["send_update_email"], text


def make_datasets_public(active_files, what_changed):
    vorlage_in_filename = [f for f in active_files if "Vorlage" in f]
    logging.info(
        f'Number of data files with "Vorlage" in the filename: {len(vorlage_in_filename)}. If 0: pushing data to public datasets.'
    )
    if len(vorlage_in_filename) == 0:
        for ods_id in ["100345", "100346"]:
            policy_changed, r = common.ods_set_general_access_policy(ods_id, False)
            if policy_changed:
                what_changed["updated_ods_datasets"].append(ods_id)
    return what_changed


def publish_datasets(details_changed, kennz_changed, what_changed):
    if kennz_changed:
        logging.info("Kennzahlen have changed, pushing data to the test dataset...")
        what_changed["updated_ods_datasets"].append("100343")
    if details_changed:
        logging.info("Details have changed, pushing data to the test dataset...")
        what_changed["updated_ods_datasets"].append("100344")
    return what_changed


def calculate_and_upload(active_files):
    details_abst_date, df_details = calculate_details(active_files)
    details_export_file_name = os.path.join(
        "data",
        "data-processing-output",
        f"Abstimmungen_Details_{details_abst_date}.csv",
    )
    details_changed = upload_ftp_if_changed(df_details, details_export_file_name)

    kennz_abst_date, df_kennz = calculate_kennzahlen(active_files)
    kennz_file_name = os.path.join("data", "data-processing-output", f"Abstimmungen_{kennz_abst_date}.csv")
    kennz_changed = upload_ftp_if_changed(df_kennz, kennz_file_name)
    return df_details, details_changed, df_kennz, kennz_changed


def have_data_files_changed(active_files):
    data_files_changed = False
    for file in active_files:
        if ct.has_changed(os.path.join("data", file)):
            data_files_changed = True
    logging.info(f"Are there any changes in the active data files? {data_files_changed}.")
    return data_files_changed


def find_data_files_for_active_abst(active_abst):
    data_files = get_latest_data_files()
    abst_datum_string = active_abst.Abstimmungs_datum[0].replace("-", "")
    active_files = [f for f in data_files if abst_datum_string in f]
    logging.info(f"We have {len(active_files)} data files for the current Abstimmung: {active_files}. ")
    return active_files


def check_embargos(active_abst, active_active_size):
    logging.info(f"Found {active_active_size} active Abstimmung.")
    for column in ["Ignore_changes_before", "Embargo", "Ignore_changes_after"]:
        active_abst[column] = active_abst[column].dt.tz_localize("Europe/Zurich")
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo("Europe/Zurich"))
    do_process = active_abst.Ignore_changes_before[0] <= now_in_switzerland < active_abst.Ignore_changes_after[0]
    make_live_public = active_abst.Embargo[0] <= now_in_switzerland < active_abst.Ignore_changes_after[0]
    return do_process, make_live_public


def upload_ftp_if_changed(df, file_name):
    print(f"Exporting to {file_name}...")
    df.to_csv(file_name, index=False)
    has_changed = ct.has_changed(file_name)
    if has_changed:
        common.upload_ftp(file_name, remote_path="wahlen_abstimmungen/abstimmungen")
    return has_changed


def get_latest_data_files():
    data_file_names = []
    for pattern in ["*_EID_????????*.xlsx", "*_KAN_????????*.xlsx"]:
        file_list = glob.glob(os.path.join("data", pattern))
        if len(file_list) > 0:
            latest_file = max(file_list, key=os.path.getmtime)
            data_file_names.append(os.path.basename(latest_file))
    return data_file_names


if __name__ == "__main__":
    print(f"Executing {__file__}...")
    main()
