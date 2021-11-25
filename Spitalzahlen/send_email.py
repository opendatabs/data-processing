import credentials_email
from make_email import message
import smtplib

def make_email_dict(email_textfile):
    d = {}
    with open(email_textfile) as f:
        for line in f:
            (key, val) = line.split()
            d[key] = val
    return d


def send_email(hospital, email_dict):
    with open('../data-processing/Spitalzahlen/Erinnerung_IES.txt', 'r') as file:
        text = file.read()
    msg = message(subject="test", text=text)
    email_receivers = email_dict[hospital].split(",")

    # initialize connection to email server
    smtp = smtplib.SMTP('smtp-mail.outlook.com', port='587')

    smtp.ehlo()  # send the extended hello to our server
    smtp.starttls()  # tell server we want to communicate with TLS encryption

    smtp.login(credentials_email.email, credentials_email.password)  # login to our email server

    # send our email message 'msg' to our boss
    smtp.sendmail(from_addr=credentials_email.email,
              to_addrs=email_receivers,
              msg=msg.as_string())
    smtp.quit()  # finally, don't forget to close the connection


#email_dict = make_email_dict('test_IES_emailadresses.txt')
#send_email(hospital='Clara', email_dict=email_dict)

