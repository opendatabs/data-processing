from gsv_covid19_hosp import credentials
from make_email import message
import smtplib



def send_email(hospital, day="today"):
    email_dict = credentials.IES_emailadresses
    text_reminder = credentials.Errinerung_IES
    email_receivers_hospital = email_dict[hospital].split(",")
    if day == "today":
        text = f"There are no entries in IES for {hospital} today, " \
            f"\n\n" \
            f"Send the message below to the following email-adresses:" \
            f"\n" \
            f"{email_receivers_hospital}" \
            f"\n\n" \
            f"{text_reminder}"
        subject = f"No IES entries {hospital} today"
    else:
        text = f"There are no entries in IES for {hospital} on {day}, " \
               f"\n\n" \
               f"Send a message to receive the numbers by email to the following email-adresses:" \
               f"\n\n" \
               f"{email_receivers_hospital}"
        subject = f"No IES entries {hospital} on {day}"

    msg = message(subject=subject, text=text)

    email_receivers = credentials.email_receivers

    # initialize connection to email server
    smtp = smtplib.SMTP(credentials.email_server_outlook, port='587')

    smtp.ehlo()  # send the extended hello to our server
    smtp.starttls()  # tell server we want to communicate with TLS encryption

    smtp.login(credentials.email, credentials.password_email)  # login to our email server

    # send our email message 'msg'
    smtp.sendmail(from_addr=credentials.email,
              to_addrs=email_receivers,
              msg=msg.as_string())
    smtp.quit()  # finally, don't forget to close the connection


if __name__ == "__main__":
    send_email('Clara')
