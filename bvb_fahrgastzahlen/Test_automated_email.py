from exchangelib import Credentials, Account, Configuration, DELEGATE
from exchangelib.folders import Inbox
from datetime import datetime
from bvb_fahrgastzahlen import credentials

# Set up account and credentials
# TODO: Still gives an SSL-Error. Fix this so the job can be fully automated.
exchangelib_credentials = Credentials(username=credentials.username, password=credentials.password)
config = Configuration(server=credentials.server, credentials=exchangelib_credentials)

# Access the account
account = Account(primary_smtp_address=credentials.username, config=config, autodiscover=False, access_type=DELEGATE)

# Define the file name
file_name = 'Fahrgastzahlen'

# Define the email domain
company_email_domain = 'bvb.ch'

# Get the start and end dates for the current month
current_month = datetime.now().month

# Retrieve emails from the inbox folder
for item in account.inbox.filter(has_attachments=True, sender__endswith=company_email_domain,
                                 datetime_received__month=current_month):
    if any(attachment.name.lower() == file_name.lower() for attachment in item.attachments):
        print(f"Found attachment '{file_name}' in email from '{item.sender.email_address}' "
              f"with subject: {item.subject} received on {item.datetime_received}")

         # Save the attachment
        attachment = item.attachments[0]  # Assume there is only one attachment
        with open(attachment.name, 'wb') as f:
            f.write(attachment.content)
