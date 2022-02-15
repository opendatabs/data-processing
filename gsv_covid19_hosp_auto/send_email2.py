import logging
import pandas as pd
from gsv_covid19_hosp_auto import credentials
from gsv_covid19_hosp_auto import make_email
import smtplib
from datetime import timezone, datetime, timedelta
from zoneinfo import ZoneInfo


def check_if_email(df_log, date, day, now_in_switzerland, time_for_email, time_for_email_to_call, time_for_email_final_status):
    pd.set_option('display.max_columns', None)
    df_missing = df_log[(df_log['time_IES_entry'] == "") & (df_log["Date"] == date)]
    if day in ["Saturday", "Sunday"]:
        if not df_missing.empty:
            for index, row in df_missing.iterrows():
                hospital = row["Hospital"]
                condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                if row['email_reminder'] == "":
                        send_email(hospital=hospital, day=day, email_type="Reminder")
                        df_log.loc[condition, 'email_reminder'] = f"Sent at {now_in_switzerland}"
                        logging.info(f'email reminder for entry {hospital} on {day} sent at {now_in_switzerland}')
                else:
                    email_send_at = df_log.loc[condition, 'email_reminder']
                    logging.info(f'email reminder for entry {hospital} on {day} has already been sent: {email_send_at} ')
    elif day == "today":
        if not df_missing.empty:
            if now_in_switzerland > time_for_email_final_status:
                if (df_log['email_status_at_10'] == "").all():
                    df_log['email_status_at_10'] = "Sent"
                    send_email(hospital=None, email_type="Not all filled at 10", day=day, df_log=df_log)
                    logging.info("if not yet done: send status email: not all filled")
            elif now_in_switzerland > time_for_email_to_call:
                for index, row in df_missing.iterrows():
                    if row['email_for_calling'] == "":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Call")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, 'email_for_calling'] = f"Sent at {now_in_switzerland}"
            elif now_in_switzerland > time_for_email:
                for index, row in df_missing.iterrows():
                    if row['email_reminder'] == "":
                        hospital = row["Hospital"]
                        send_email(hospital=hospital, day=day, email_type="Reminder")
                        condition = (df_log["Date"] == date) & (df_log["Hospital"] == hospital)
                        df_log.loc[condition, 'email_reminder'] = f"Sent at {now_in_switzerland}"
        elif df_missing.empty:
            # check if really all has been filled completely
            if (df_log['CoReport_filled'] == "Yes").all():
                if df_log['all_filled'].sum() == 0:
                    df_log['all_filled'] = 1
                    send_email(hospital=None, email_type="All filled", day=day, df_log=df_log)
                    logging.info(f"Send email: everything ok at {now_in_switzerland}")
                    df_log['email_all_filled'] = f"Sent at {now_in_switzerland}"
    return df_log


def send_email(hospital, email_type, day="today", extra_info = [], df_log=None, attachment=None, html_content=None):
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
        else:
            text = f"There are no entries in IES for {hospital} on {day}, " \
                   f"\n\n" \
                   f"Send a message to receive the numbers by email to the following email-addresses:" \
                   f"\n\n" \
                   f"{email_receivers_hospital}"
            subject = f"No IES entries {hospital} on {day}"
    elif email_type == "Call":
        phone_hospital = phone_dict[hospital]
        text =  f"There are still no entries in IES for {hospital} today, " \
                   f"\n\n" \
                   f"Please call:" \
                   f"\n\n" \
                   f"{phone_hospital}"
        subject = f"Still no IES entries {hospital} today"
    elif email_type == "Not all filled at 10":
        logging.info("Send email with log file, message whether all is filled or not")
        subject="Warning: CoReport has not been filled completely before 10"
        text = "Please find in the attachment today's log file."
        attachment = credentials.path_log_csv
        html_content = df_log.to_html()
    elif email_type == "All filled":
        subject= "CoReport all filled"
        text = "All values of today have been entered into CoReport." \
               "\n" \
               "Please find in the attachment today's log file."
        attachment = credentials.path_log_csv
        html_content = df_log.to_html()
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

    msg = make_email.message(subject=subject, text=text, attachment=attachment, html_content=html_content)

    # initialize connection to email server
    host = credentials.email_server
    smtp = smtplib.SMTP(host)

    # send email
    smtp.sendmail(from_addr=credentials.email,
                  to_addrs=credentials.email_receivers,
                  msg=msg.as_string())
    smtp.quit()  # finally, don't forget to close the connection

