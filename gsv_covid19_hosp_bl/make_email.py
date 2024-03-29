import os
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart


# copied from: https://towardsdatascience.com/automate-email-with-python-1e755d9c6276
def message(subject="Python Notification", text="", img=None, html_content=None, attachment=None):
    # build message contents
    # 'mixed' so that both text and html are displayed in email
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject  # add in the subject
    # msg.attach(MIMEText(text))  # add text contents
    msg.attach(MIMEText(text, 'plain', 'utf-8'))  # add plain text contents
    # add html content
    if html_content is not None:
        msg_html_content = MIMEText(html_content, 'html', 'utf-8')
        msg.attach(msg_html_content)

    # check if we have anything given in the img parameter
    if img is not None:
        # if we do, we want to iterate through the images, so let's check that
        # what we have is actually a list
        if type(img) is not list:
            img = [img]  # if it isn't a list, make it one
        # now iterate through our list
        for one_img in img:
            img_data = open(one_img, 'rb').read()  # read the image binary data
            # attach the image data to MIMEMultipart using MIMEImage, we add
            # the given filename use os.basename
            msg.attach(MIMEImage(img_data, name=os.path.basename(one_img)))

    # we do the same for attachments as we did for images
    if attachment is not None:
        if type(attachment) is not list:
            attachment = [attachment]  # if it isn't a list, make it one

        for one_attachment in attachment:
            with open(one_attachment, 'rb') as f:
                # read in the attachment using MIMEApplication
                file = MIMEApplication(
                    f.read(),
                    name=os.path.basename(one_attachment)
                )
            # here we edit the attached file metadata
            file['Content-Disposition'] = f'attachment; filename="{os.path.basename(one_attachment)}"'
            msg.attach(file)  # finally, add the attachment to our message object
    return msg
