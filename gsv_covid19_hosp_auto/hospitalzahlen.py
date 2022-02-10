""""
get_data.check_day(): check which day it is, differentiate between monday and other weekdays

for all days that need to be filled: at 9:15 get_df_for_date(date, list_hospitals, weekend), where weekend is boolean
get_df_for_date returns dataframe with the latest entry of the day for each hospital (if available)

To be changed..:if no data there, send email, check again after 15 minutes and get data, if still no data, give warning

Calculate needed numbers:
calculation.calculate_numbers(ies_numbers): takes dataframe of the day
and returns the dataframe df_coreport which contains the numbers to be entered into CoReport:
[Betten_frei_Normal, Betten_frei_Normal_COVID, Betten_frei_IMCU, Betten_frei_IPS_ohne_Beatmung,
      Betten_frei_IPS_mit_Beatmung, Betten_frei_ECMO, Betten_belegt_Normal, Betten_belegt_IMCU, Betten_belegt_IPS_ohne_Beatmung,
      Betten_belegt_IPS_mit_Beatmung, Betten_belegt_ECMO]

Get value id's from CoReport
coreport_scraper.make_df_value_id(date, list_hospitals) adds the value id's to df_coreport
(alteratively, if no longer on the website,
the pickle file with the value id's stored by coreport_scraper.add_value_id(df, date) can be joined with
df_coreport)


Write into CoReport:
update_coreport.write_in_coreport(df, hospital_list, date) executes calculation.calculate_numbers
and coreport_scraper.add_value_id, and then enters the numbers into CoReport
"""


import pandas as pd
from gsv_covid19_hosp_auto import get_data
from gsv_covid19_hosp_auto import send_email
from gsv_covid19_hosp_auto import calculation
from datetime import timezone, datetime, timedelta
import logging
from gsv_covid19_hosp_auto import credentials
from gsv_covid19_hosp_auto import update_coreport, send_email2
from zoneinfo import ZoneInfo


def all_together(date, list_hospitals):
    check_for_log_file(date, day_of_week, list_hospitals)
    df_log = pd.read_pickle("log_file.pkl")
    day_of_week = get_data.check_day(date)
    if day_of_week == "Monday":
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date - timedelta(2), day="Saturday",
                                          list_hospitals=list_hospitals, weekend=True)
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date - timedelta(1), day="Sunday",
                                          list_hospitals=list_hospitals, weekend=True)
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date, day="today", list_hospitals=list_hospitals,
                                          weekend=False)
        # send emails if values missing for Saturday or Sunday
        df_log = send_email2.check_if_email(df_log=df_log, date=date - timedelta(2), day="Saturday")
        df_log = send_email2.check_if_email(df_log=df_log, date=date - timedelta(1), day="Sunday")
    elif day_of_week == "Other workday":
        df_log = try_to_enter_in_coreport(df_log=df_log, date=date, day="today", list_hospitals=list_hospitals, weekend=False)
    else:
        logging.info("It is weekend")
    df_log = send_email2.check_if_email(df_log=df_log, date=date, day="today")
    df_log.to_pickle("log_file.pkl")
    df_log.to_csv("log_file.csv", index=False)


def check_for_log_file(date, day_of_week, list_hospitals):
    try:
        with open("log_file.csv") as log_file:
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
    df["IES entry"] = ""
    df["CoReport filled"] = ""
    df["email negative value"] = ""
    df["email reminder"] = ""
    df["email for calling"] = ""
    df["email status at 10"] = ""
    df["email: all ok"] = ""
    df["all filled"] = 0
    #df.set_index("Date", inplace=True)
    df.to_pickle("log_file.pkl")


def try_to_enter_in_coreport(df_log, date, day, list_hospitals, weekend):
    logging.info(f"Read out data for {day} in IES system")
    df, missing = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=weekend)
    if not df.empty:
        filled_hospitals = [x for x in list_hospitals if x not in missing]
        logging.info(f"Add entries of {filled_hospitals} for {day} into CoReport")
        df_log = update_coreport.write_in_coreport(df, filled_hospitals, date=date,day=day, df_log=df_log)
        logging.info(f"Entries added into CoReport for {filled_hospitals}")
        logging.info(f"There are no entries of {missing} for {day} in IES")
        for hospital in filled_hospitals:
            row_hospital = df[df["Hospital"] == hospital]
            timestamp = row_hospital["CapacTime"].values[0]
            condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
            df_log.loc[condition, "IES entry"] = timestamp
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
    pd.set_option('display.max_columns', None)
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    date = now_in_switzerland.date()
    list_hospitals = ['USB', 'Clara', 'UKBB']
    all_together(date=date, list_hospitals=list_hospitals)

