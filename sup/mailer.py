import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class Mailer():
    def __init__(self, host, port, user, password, admins):
        self.host = host
        self.user = user
        self.port = port
        self.password = password

    def notify(self, subject, body, recipients):
        """
        Send an e-mail notification.
        """
        from_addr = self.user

        # Construct the message.
        msg = MIMEMultipart()
        msg['From'] = from_addr
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Connect to the mail server.
        server = smtplib.SMTP(self.host, self.port)
        server.starttls()
        server.login(from_addr, self.password)

        for target in recipients:
            msg['To'] = target
            server.sendmail(from_addr, target, msg.as_string())

        server.quit()
