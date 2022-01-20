import logging

from gsv_covid19_hosp import credentials
from gsv_covid19_hosp import make_email
import smtplib
from datetime import timezone, datetime, timedelta
from zoneinfo import ZoneInfo


def check_if_email(df_log, date, day):
    now_in_switzerland = datetime.now(timezone.utc).astimezone(ZoneInfo('Europe/Zurich'))
    date = now_in_switzerland.date()
    time = now_in_switzerland.time().replace(microsecond=0)
    time_for_email = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=30,
                              tzinfo=ZoneInfo('Europe/Zurich'))
    time_for_email_to_call = datetime(year=date.year, month=date.month, day=date.day, hour=9, minute=50,
                                      tzinfo=ZoneInfo('Europe/Zurich'))
    time_for_email_final_status = datetime(year=date.year, month=date.month, day=date.day, hour=10, minute=0,
                                           tzinfo=ZoneInfo('Europe/Zurich'))
    df_missing = df_log[(df_log["IES entry"] == "No entry") & (df_log["Date"] == date)]
    if day in ["Saturday", "Sunday"]:
        if not df_missing.empty():
            for row in df_missing:
                if row["email reminder"] == "-":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Reminder")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, "email reminder"] = f"send at {time}"
    else:
        if not df_missing.empty():
            if time > time_for_email_final_status:
                print("if not yet done: send status email, not all filled")
            elif time > time_for_email_to_call:
                for row in df_missing:
                    if row["email for calling"] == "-":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Call")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, "email for calling"] = f"send at {time}"
            elif time > time_for_email:
                for row in df_missing:
                    if row["email reminder"] == "-":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Reminder")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, "email reminder"] = f"send at {time}"
        else:
            print("check if mail all filled has been sent, if not: send it")
    return df_log

def send_email(hospital, email_type, day="today"):
    # to do: add text, subject and receivers for each type of email, for status email add attachment or print log file..
    if email_type == "Reminder":
        email_dict = credentials.IES_emailadresses
        text_reminder = credentials.Errinerung_IES
        email_receivers_hospital = email_dict[hospital].split(",")
        if day == "today":
            text = f"There are no entries in IES for {hospital} today, " \
                f"\n\n" \
                f"Send the message below to the following email-addresses:" \
                f"\n" \
                f"{email_receivers_hospital}" \
                f"\n\n" \
                f"{text_reminder}"
            subject = f"No IES entries {hospital} today"
        else:
            text = f"There are no entries in IES for {hospital} on {day}, " \
                   f"\n\n" \
                   f"Send a message to receive the numbers by email to the following email-addresses:" \
                   f"\n\n" \
                   f"{email_receivers_hospital}"
            subject = f"No IES entries {hospital} on {day}"
    elif email_type == "Call":
        phone_hospital = []
        text =  f"There are still no entries in IES for {hospital} today, " \
                   f"\n\n" \
                   f"Please call:" \
                   f"\n\n" \
                   f"{phone_hospital}"
    elif email_type == "Status":
        logging.info("Send email with log file, message whether all is filled or not")
    elif email_type == "All filled":
        logging.info("Email send if all is filled after 10..")



    msg = make_email.message(subject=subject, text=text)


    host = "mail.bs.ch"
    smtp = smtplib.SMTP(host)


    smtp.sendmail(from_addr='hester.pieters@bs.ch',
              to_addrs=['hester.pieters@bs.ch'],
              msg=msg.as_string())
    smtp.quit()  # finally, don't forget to close the connection


if __name__ == "__main__":
    send_email('Clara')
