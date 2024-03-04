from django.core.management.base import BaseCommand
import logging
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException
from syracuse.settings import BREVO_API_KEY
from syracuse.settings import TRACKED_ORG_ACTIVITIES_DAYS
from trackeditems.notification_helpers import create_email_notifications
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class Command(BaseCommand):

    def handle(self, *args, **options):
        do_send_recent_activities_email()

def do_send_recent_activities_email():
    logger.info("Started do_send_recent_activities_email")
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = BREVO_API_KEY
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))
    subject = "[Syracuse Updates] Latest Tracked Organization Updates"
    sender = {"name":"Syracuse from 1145","email":"syracuse+mailer@1145.am"}
    reply_to = {"name":"Syracuse from 1145","email":"syracuse+mailer@1145.am"}
    email_count = 0
    email_contents = create_email_notifications(TRACKED_ORG_ACTIVITIES_DAYS)
    for user,html_content, activity_notification in email_contents:
        to = [{"email":user.email,"name":user.username}]
        send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(to=to, reply_to=reply_to, html_content=html_content, sender=sender, subject=subject)
        try:
            api_response = api_instance.send_transac_email(send_smtp_email)
            activity_notification.sent_at = datetime.now(tz=timezone.utc)
            logger.info(api_response)
            activity_notification.save()
            email_count += 1
        except ApiException as e:
            logger.error(f"Exception when calling SMTPApi->send_transac_email: {e}")
    return email_count
