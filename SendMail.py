from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib


def send_email_365():

    Mail_from = "vikram@domain.com"
    Mail_to = "smahadevan@domain.com"

     # Declare SMTP credentials
    password = "xxxxxxxxx"
    username = "vikram@domain.com"
    smtphost = "smtp.office365.com:587"

    # Create message object instance
    msg = MIMEMultipart()

    # Create message body
    message = "Hi saran, sending email via python working fine!!!!"

   
    # Declare message elements
    msg['From'] = Mail_from
    msg['To'] = Mail_to
    msg['Subject'] = "Test from Python via AuthSMTP"

    # Add the message body to the object instance
    msg.attach(MIMEText(message))

    server = smtplib.SMTP(smtphost)
    server.starttls()
    server.login(username, password)
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()
    print("Successfully sent email message to %s:" % (msg['To']))

send_email_365()
