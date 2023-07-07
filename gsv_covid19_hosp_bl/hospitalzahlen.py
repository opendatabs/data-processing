import pandas as pd
from gsv_covid19_hosp_bl import get_data
from datetime import timezone, datetime, timedelta
import logging
from gsv_covid19_hosp_bl import update_coreport, send_email2, credentials
from zoneinfo import ZoneInfo

# hospitals to be filled
list_hospitals = ['Bruderholz', 'Liestal']

# current time and date
now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
date = now_in_switzerland.date()

# time conditions
time_for_email = now_in_switzerland.replace(hour=9, minute=35, second=0, microsecond=0)
time_for_email_to_call = now_in_switzerland.replace(hour=9, minute=55, second=0, microsecond=0)
time_for_email_final_status = now_in_switzerland.replace(hour=12, minute=0, second=0, microsecond=0)
starting_time = now_in_switzerland.replace(hour=9, minute=0, second=0, microsecond=0)

# To do the same on all day and ignore the different weekend case for bs just define day_of_week = "Other workday"
day_of_week = "Other workday"

def main(date, day_of_week, list_hospitals):
    check_for_log_file(date, day_of_week, list_hospitals)
    df_log = pd.read_pickle(credentials.path_log_pkl)
    if day_of_week == "Monday":
        hospitals_left = hospitals_left_to_fill(date=date-timedelta(2), df_log=df_log)
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date - timedelta(2), day="Saturday",
                                          list_hospitals=hospitals_left, weekend=True)
        hospitals_left = hospitals_left_to_fill(date=date-timedelta(1), df_log=df_log)
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date - timedelta(1), day="Sunday",
                                          list_hospitals=hospitals_left, weekend=True)
        hospitals_left = hospitals_left_to_fill(date=date, df_log=df_log)
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date, day="today", list_hospitals=hospitals_left,
                                          weekend=False)
        # send emails if values missing for Saturday or Sunday
        df_log = send_email2.check_if_email(df_log=df_log, date=date - timedelta(2), day="Saturday",
                                            now_in_switzerland=now_in_switzerland,
                                            time_for_email=time_for_email,
                                            time_for_email_to_call=time_for_email_to_call,
                                            time_for_email_final_status=time_for_email_final_status)
        df_log = send_email2.check_if_email(df_log=df_log, date=date - timedelta(1), day="Sunday",
                                            now_in_switzerland=now_in_switzerland,
                                            time_for_email=time_for_email,
                                            time_for_email_to_call=time_for_email_to_call,
                                            time_for_email_final_status=time_for_email_final_status)
    elif day_of_week == "Other workday":
        hospitals_left = hospitals_left_to_fill(date=date, df_log=df_log)
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date, day="today",
                                          list_hospitals=hospitals_left, weekend=False)
    df_log = send_email2.check_if_email(df_log=df_log, date=date, day="today",
                                        now_in_switzerland=now_in_switzerland,
                                        time_for_email=time_for_email,
                                        time_for_email_to_call=time_for_email_to_call,
                                        time_for_email_final_status=time_for_email_final_status)
    df_log.to_pickle(credentials.path_log_pkl)
    df_log.to_csv(credentials.path_log_csv, index=False)


def hospitals_left_to_fill(date, df_log):
    condition2 = (df_log["Date"] == date) & (df_log['CoReport_filled'] == "Yes")
    already_filled = df_log.loc[condition2, "Hospital"]
    logging.info(f'CoReport has been filled for {already_filled} on {date}')
    condition = (df_log["Date"] == date) & (df_log['CoReport_filled'] != "Yes")
    hospitals_left = df_log.loc[condition, "Hospital"]
    logging.info(f'Still need to fill CoReport for {hospitals_left} on {date}')
    return hospitals_left


def check_for_log_file(date, day_of_week, list_hospitals):
    try:
        with open(credentials.path_log_csv) as log_file:
            df_log = pd.read_csv(log_file)
            if str(date) not in list(df_log["Date"]):
                make_log_file(date, day_of_week, list_hospitals)
    except OSError:
        make_log_file(date, day_of_week, list_hospitals)


def make_log_file(date, day_of_week, list_hospitals):
    df = pd.DataFrame()
    numb_hosp = len(list_hospitals)
    if day_of_week == "Monday":
        df["Date"] = [date - timedelta(2)] * numb_hosp + [date - timedelta(1)] * numb_hosp + [date] * numb_hosp
        df["Hospital"] = list_hospitals * 3
    else:
        df["Date"] = [date] * numb_hosp
        df["Hospital"] = list_hospitals
    df['time_IES_entry'] = ""
    df['CoReport_filled'] = ""
    df['email_negative_value'] = ""
    df['email_reminder'] = ""
    df['email_for_calling'] = ""
    df['email_status_at_12'] = ""
    df['email_all_filled'] = ""
    df['all_filled'] = 0
    df.to_pickle(credentials.path_log_pkl)


def try_to_enter_in_coreport(df_log, date, day, list_hospitals, weekend):
    logging.info(f"Read out data for {day} in IES system")
    df, missing = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=weekend)
    if not df.empty:
        filled_hospitals = [x for x in list_hospitals if x not in missing]
        logging.info(f"Add entries of {filled_hospitals} for {day} into CoReport")
        df_log = update_coreport.write_in_coreport(df, filled_hospitals, date=date, day=day, df_log=df_log)
        logging.info(f"Entries added into CoReport for {filled_hospitals}")
        logging.info(f"There are no entries of {missing} for {day} in IES")
        for hospital in filled_hospitals:
            row_hospital = df[df["Hospital"] == hospital]
            timestamp = row_hospital["CapacTime"].values[0]
            condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
            df_log.loc[condition, 'time_IES_entry'] = timestamp
        logging.info(f"There are no entries of {missing} for {day} in IES")
    return df_log


def get_df_for_date(date, list_hospitals, weekend=False):
    df = pd.DataFrame()
    missing_hospitals = []
    for hospital in list_hospitals:
        result = get_df_for_date_hospital(hospital=hospital, date=date, weekend=weekend)
        if result.empty:
            missing_hospitals.append(hospital)
        else:
            result['Hospital'] = hospital
            df = pd.concat([df, result])
    return df, missing_hospitals


def get_df_for_date_hospital(hospital, date, weekend=False):
    df_entries = get_data.get_dataframe(hospital=hospital, date=date)
    number_of_entries = df_entries.shape[0]
    logging.info(f"Check if there were actually entries for {hospital} on {date}")
    if number_of_entries == 0:
        if weekend:
            logging.warning(f"Numbers for the weekend day {date} are not available for {hospital}!")
            return pd.DataFrame()
        else:
            logging.warning(f"Numbers for {hospital} for {date} are not available...")
            return pd.DataFrame()
    elif number_of_entries >= 1:
        logging.info(f"There is at least one entry for {hospital} on {date}.")
        latest_entry = df_entries.CapacTime.max()
        logging.info(f"We take the latest entry for {hospital}, which was at {latest_entry}.")
        df_entry = df_entries[df_entries.CapacTime == df_entries.CapacTime.max()]
        return df_entry


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.info(f'Executing {__file__}...')
    do_process = now_in_switzerland >= starting_time
    logging.info(f'Checking if we have to do anything right now: {do_process}')
    if do_process:
        logging.info(f"OK, let's start processing the data!")
        main(date=date, day_of_week=day_of_week, list_hospitals=list_hospitals)

    # print out results for checking:
    # from gsv_covid19_hosp_bl import calculation
    # pd.set_option('display.max_columns', 500)
    # hospital = "Bruderholz"
    # df_entries = get_df_for_date(list_hospitals=list_hospitals, date=date)
    # print(df_entries[0])
    # df_coreport = calculation.calculate_numbers(df_entries[0])
    # print(df_coreport)
    # df = update_coreport.add_value_id(df_coreport, date)
    # print(df)
