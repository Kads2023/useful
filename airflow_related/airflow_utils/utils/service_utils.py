import smtplib
import logging


class ServiceUtils:

    @classmethod
    def send_email(cls, error_message, task_id, dag_id):
        sender_email = "alert@dmp-airflow.com"
        receiver_email = ["nidhshah@paypal.com","sasingarayar@paypal.com","lidhiraviamuthu@paypal.com"]
        emails =[]
        for email in emails:
            receiver_email.append(email)

        mail_message = "Data Movement Job Failure"
        components = [mail_message, dag_id, task_id]
        email_html = cls.email_content(error_message)

        message = MIMEMultipart("multipart")
        part2 = MIMEText(email_html, "html")
        message.attach(part2)
        message["Subject"] = " | ".join(components)

        message["From"] = sender_email

        for i, val in enumerate(receiver_email):
            message["To"] = val

        logging.info(f"Message to be sent in email alert: {message}")
        server = smtplib.SMTP("mx.phx.paypal.com")
        #server.connect()
        logging.info("Email Sent")
        server.sendmail(sender_email, receiver_email, message.as_string())

    @classmethod
    def email_content(cls, error_message):
        """ Email Content for Alerts"""
        replace_mail_content = []
        replace_mail_content.append("You are receiving this alert because the Job has failed")
        replace_mail_content.append("The critical job mentioned below has failed.")
        replace_mail_content.append("")
        email_body = """
                    <html>
                        <body>
                            <p>Hi team,</p>
                            <br>
                            <p><b style="font-size:18px";>What is this alert for?</b></p>
                            <p>""" + str(replace_mail_content[0]) + """</p>
                            <br>
                            <p><b style="font-size:18px";>What has Happened?</b></p>
                            <p>""" + str(replace_mail_content[1]) + """</p>
                            <p>""" + str(replace_mail_content[2]) + """</p>
                            <br>
                            <p><b>Message:</b> """ + str(error_message) + """</p>
                            <p><b style="font-size:18px";>How to deal with it?</b></p>
                            <p>This is just a notification alert.</p>
                            <p>The Data Movement Support team is working to resolve it</p>
                            <br>
                            <p>Regards,</p>
                            <p>Team</p>
                        </body>
                    </html>
                  """

        return email_body


def read_json(config_json, qualified_name_list, default_value=None):
    if config_json is None or not qualified_name_list:
        return default_value

    cfg = config_json
    for key in qualified_name_list:
        if key not in cfg:
            return default_value
        cfg = cfg[key]

    return cfg



def merge_configs(original_config, new_config):
    for k in new_config:
        if k in original_config:
            if isinstance(new_config[k], dict) and isinstance(original_config[k], dict):
                original_config[k] = merge_configs(original_config[k], new_config[k])
            elif (not isinstance(new_config[k], dict) and isinstance(original_config[k], dict)) or (isinstance(new_config[k], dict) and not  isinstance(original_config[k], dict)):
                raise Exception(f"Cannot merge configs for key: {k}")
            else:
                original_config[k] = new_config[k]
        else:
            original_config[k] = new_config[k]

    return original_config