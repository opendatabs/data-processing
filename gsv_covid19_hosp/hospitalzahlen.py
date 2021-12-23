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
from gsv_covid19_hosp import get_data
from gsv_covid19_hosp import send_email
from gsv_covid19_hosp import calculation
from datetime import timezone, datetime, timedelta
import logging
from gsv_covid19_hosp import credentials
from gsv_covid19_hosp import update_coreport

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

def all_together(date, list_hospitals):
    day_of_week = get_data.check_day(date)
    if day_of_week == "Monday":
        saturday = date-timedelta(2)
        logging.info("Read out data from Saturday in IES system")
        df_saturday, missing_saturday = get_df_for_date(date=saturday, list_hospitals=list_hospitals, weekend=True)
        if df_saturday.empty == False:
            list_hospitals_sat = [x for x in list_hospitals if x not in missing_saturday]
            logging.info(f"Add Saturday entries of {list_hospitals_sat} into CoReport")
            update_coreport.write_in_coreport(df_saturday, list_hospitals_sat, date=saturday)
            logging.info(f"Entries added into CoReport for {list_hospitals_sat}")
            logging.info(f"There are no entries on Saturday for {missing_saturday} in IES")
            if not not missing_saturday:
                for hospital in missing_saturday:
                    logging.info(f"send email for missing entries {hospital} on Saturday")
                    send_email.send_email(hospital=hospital, day="Saturday")
        elif df_saturday.empty == True:
            logging.info(f"There are no entries on Saturday in the IES system")
            for hospital in missing_saturday:
                logging.info(f"send email for missing entries {hospital} on Saturday")
                send_email.send_email(hospital=hospital, day="Saturday")
        sunday = date - timedelta(1)
        df_sunday, missing_sunday = get_df_for_date(date=sunday, list_hospitals=list_hospitals, weekend=True)
        if df_sunday.empty == False:
            list_hospitals_sun = [x for x in list_hospitals if x not in missing_sunday]
            logging.info(f"Add Sunday entries of {list_hospitals_sun} into CoReport")
            update_coreport.write_in_coreport(df_sunday, list_hospitals_sun, date=sunday)
            logging.info(f"Entries added into CoReport for {list_hospitals_sun}")
            logging.info(f"There are no entries on Sunday for {missing_sunday} in IES")
            if not not missing_sunday:
                for hospital in missing_sunday:
                    logging.info(f"send email for missing entries {hospital} on Sunday")
                    send_email.send_email(hospital=hospital, day="Sunday")
        elif df_sunday.empty == True:
            logging.info(f"There are no entries on Sunday in the IES system")
            for hospital in missing_sunday:
                logging.info(f"send email for missing entries {hospital} on Sunday")
                send_email.send_email(hospital=hospital, day="Sunday")
        df_monday, missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if df_monday.empty == False:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            logging.info(f"Add today's entries of {filled_hospitals} into CoReport")
            update_coreport.write_in_coreport(df_monday, filled_hospitals, date=date)
            logging.info(f"Entries added into CoReport for {filled_hospitals}")
            logging.info(f"There are no entries today for {missing_hospitals} in IES")
            if not not missing_hospitals and now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    send_email.send_email(hospital=hospital)
        elif df_monday.empty == True:
            logging.info(f"There are no entries in the IES system")
            if now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    send_email.send_email(hospital=hospital)
    elif day_of_week == "Other workday":
        df, missing_hospitals = get_df_for_date(date=date, list_hospitals=list_hospitals, weekend=False)
        if df.empty == False:
            filled_hospitals = [x for x in list_hospitals if x not in missing_hospitals]
            logging.info(f"Add today's entries of {filled_hospitals} into CoReport")
            update_coreport.write_in_coreport(df, filled_hospitals, date=date)
            logging.info(f"Entries added into CoReport for {filled_hospitals}")
            logging.info(f"There are no entries today for {missing_hospitals} in IES")
            if not not missing_hospitals and now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    send_email.send_email(hospital=hospital)
        elif df.empty == True:
            logging.info("There are no entries today in IES")
            if now_in_switzerland > time_for_email:
                for hospital in missing_hospitals:
                    logging.info(f"send email for missing entries {hospital} today")
                    send_email.send_email(hospital=hospital)
    else:
        logging.info("It is weekend")


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
    time_for_email = datetime(year=date.year, month=date.month, day=date.day, hour=8, minute0)
    list_hospitals = ['USB', 'Clara', 'UKBB']
    all_together(date=date, list_hospitals=list_hospitals)

