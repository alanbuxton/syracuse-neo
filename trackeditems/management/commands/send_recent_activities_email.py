from django.core.management.base import BaseCommand
import logging
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from syracuse.settings import BREVO_API_KEY
from trackeditems.notification_helpers import create_email_notifications

logger = logging.getLogger(__name__)

class Command(BaseCommand):

    def handle(self, *args, **options):
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = BREVO_API_KEY
        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
        subject = "[Tracked Organizations] Latest Updates from Syracuse by 1145"
        sender = {"name":"Syracuse by 1145","email":"syracuse+mailer@1145.am"}
        reply_to = {"name":"Syracuse by 1145","email":"syracuse+mailer@1145.am"}
        email_contents = create_email_notifications(7)
        for user,html_content in email_contents:
            to = [{"email":user.email,"name":user.username}]
            send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, reply_to=reply_to, html_content=html_content, sender=sender, subject=subject)
            try:
                api_response = api_instance.send_transac_email(send_smtp_email)
                logger.info(api_response)
            except ApiException as e:
                logger.error(f"Exception when calling SMTPApi->send_transac_email: {e}")
