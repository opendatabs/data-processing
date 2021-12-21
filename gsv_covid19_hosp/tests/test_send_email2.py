from gsv_covid19_hosp import credentials
from gsv_covid19_hosp import make_email
import smtplib



def send_email(hospital, day="today"):
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

    msg = make_email.message(subject=subject, text=text)


    host = "mail.bs.ch"
    smtp = smtplib.SMTP(host)


    smtp.sendmail(from_addr='hester.pieters@bs.ch',
              to_addrs=['hester.pieters@bs.ch', 'jonas.bieri@bs.ch'],
              msg=msg.as_string())
    smtp.quit()  # finally, don't forget to close the connection


if __name__ == "__main__":
    send_email('Clara')
