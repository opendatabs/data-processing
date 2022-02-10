import logging
import os

import pandas as pd

from gsv_covid19_hosp_auto import testspital
from gsv_covid19_hosp_auto import credentials
from gsv_covid19_hosp_auto import make_email
import smtplib
from datetime import timezone, datetime, timedelta, time
from zoneinfo import ZoneInfo


def check_if_email(df_log, date, day, current_time=datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich')).time().replace(microsecond=0)):
    pd.set_option('display.max_columns', None)
    time_for_email = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=30,
                             tzinfo=ZoneInfo('Europe/Zurich'))
    time_for_email_to_call = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=50,
                                     tzinfo=ZoneInfo('Europe/Zurich'))
    time_for_email_final_status = datetime(year=date.year, month=date.month, day=date.day, hour=10, minute=0,
                                          tzinfo=ZoneInfo('Europe/Zurich'))
    df_missing = df_log[(df_log["IES entry"] == "") & (df_log["Date"] == date)]
    if day in ["Saturday", "Sunday"]:
        if not df_missing.empty:
            for index, row in df_missing.iterrows():
                if row["email reminder"] == "":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Reminder")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, "email reminder"] = f"Sent at {current_time}"
    elif day == "today":
        if not df_missing.empty:
            if current_time > time_for_email_final_status:
                if (df_log["email status at 10"] == "").all():
                    df_log["email status at 10"] = "Sent"
                    df_log.to_csv("log_file.csv", index=False)
                    send_email(hospital=None, email_type="Not all filled at 10", day=day)
                    logging.info("if not yet done: send status email: not all filled")
            elif current_time > time_for_email_to_call:
                for index, row in df_missing.iterrows():
                    if row["email for calling"] == "":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Call")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, "email for calling"] = f"Sent at {current_time}"
            elif current_time > time_for_email:
                for index, row in df_missing.iterrows():
                    if row["email reminder"] == "":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Reminder")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, "email reminder"] = f"Sent at {current_time}"
        elif df_missing.empty:
            # check if really all has been filled completely
            if (df_log["CoReport filled"] == "Yes").all():
                if df_log["all filled"].sum() == 0:
                    df_log["all filled"] = 1
                    df_log.to_csv("log_file.csv", index=False)
                    send_email(hospital=None, email_type="All filled", day=day)
                    logging.info(f"Send email: everything ok at {current_time}")
                    df_log["email: all ok"] = f"Sent at {current_time}"
    return df_log

def send_email(hospital, email_type, day="today", extra_info = []):
    phone_dict = credentials.IES_phonenumbers
    email_dict = credentials.IES_emailadresses
    if email_type == "Reminder":
        text_reminder = credentials.Errinerung_IES
        email_receivers_hospital = email_dict[hospital]
        if day == "today":
            text = f"There are no entries in IES for {hospital} today, " \
                f"\n\n" \
                f"Send the message below to the following email-addresses:" \
                f"\n" \
                f"{email_receivers_hospital}" \
                f"\n\n" \
                f"{text_reminder}"
            subject = f"No IES entries {hospital} today"
            attachment = None
        else:
            text = f"There are no entries in IES for {hospital} on {day}, " \
                   f"\n\n" \
                   f"Send a message to receive the numbers by email to the following email-addresses:" \
                   f"\n\n" \
                   f"{email_receivers_hospital}"
            subject = f"No IES entries {hospital} on {day}"
            attachment = None
    elif email_type == "Call":
        phone_hospital = phone_dict[hospital]
        text =  f"There are still no entries in IES for {hospital} today, " \
                   f"\n\n" \
                   f"Please call:" \
                   f"\n\n" \
                   f"{phone_hospital}"
        subject = f"Still no IES entries {hospital} today"
        attachment = None
    elif email_type == "Not all filled at 10":
        logging.info("Send email with log file, message whether all is filled or not")
        subject="Warning: CoReport has not been filled completely before 10"
        text = "Please find in the attachment today's log file."
        attachment = "log_file.csv"
    elif email_type == "All filled":
        subject= "CoReport all filled"
        text = "All values of today have been entered into CoReport." \
               "\n" \
               "Please find in the attachment today's log file."
        attachment = "log_file.csv"
        logging.info("Email if all is filled after 10..")
    elif email_type == "Negative value":
        print("here")
        prop = extra_info[0]
        hospital = extra_info[1]
        email_receivers_hospital = email_dict[hospital]
        phone_hospital = phone_dict[hospital]
        logging.info(f"Send email informing about negative value for {prop} of {hospital} ")
        subject = f"Warning: obtained negative value for {prop} of {hospital}."
        text = f"Nothing has been entered for {prop} of {hospital} since a negative value was obtained." \
               f"\n" \
               f"Please check with hospital." \
               f"   \n\n" \
               f"Email:" \
               f"\n"\
               f"{email_receivers_hospital}" \
               f"   \n\n" \
               f"Phone:" \
               f"\n" \
               f"{phone_hospital}"

    msg = make_email.message(subject=subject, text=text, attachment=attachment)

    # initialize connection to email server
    host = credentials.email_server
    smtp = smtplib.SMTP(host)

    # send email
    smtp.sendmail(from_addr=credentials.email,
                  to_addrs=credentials.email_receivers,
                  msg=msg.as_string())
    smtp.quit()  # finally, don't forget to close the connection


if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    date = datetime.today().date() - timedelta(3)
    day_of_week = "Other workday"
    list_hospitals = ['Clara', 'USB', 'UKBB']
    df_log = pd.read_csv("log_file.csv", keep_default_na=False)
    print(df_log)
    df_missing = df_log[(df_log["IES entry"] == None) & (df_log["Date"] == date)]
    print(df_missing)
    # send_email(hospital='Clara', email_type="Call")
    # send_email(None, email_type="Negative value", extra_info=[ '1', 'Clara'])