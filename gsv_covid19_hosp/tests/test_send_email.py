import smtplib

host = "mail.bs.ch"
server = smtplib.SMTP(host)
FROM = "jonas.bieri@bs.ch"
TO = "jonas.bieri@bs.ch"
MSG = "Subject: Test email mit python\n\nGuten Morgen Jonas, kommt dieses Email an? LG, J."
server.sendmail(FROM, TO, MSG)

server.quit()
print ("Email sent.")