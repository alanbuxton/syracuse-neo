import logging
from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend
from sib_api_v3_sdk import TransactionalEmailsApi, ApiClient, Configuration
from sib_api_v3_sdk.models import SendSmtpEmail, SendSmtpEmailTo

logger = logging.getLogger(__name__)

class BrevoEmailBackend(BaseEmailBackend):
    def __init__(self, fail_silently=False, **kwargs):
        super().__init__(fail_silently=fail_silently, **kwargs)

        api_key = getattr(settings, "BREVO_API_KEY", None)
        if not api_key:
            raise ValueError("BREVO_API_KEY must be set in your Django settings")

        self.default_from_email = getattr(settings, "DEFAULT_FROM_EMAIL", None)
        self.default_from_name = "Syracuse from 1145"

        configuration = Configuration()
        configuration.api_key['api-key'] = api_key
        self.api_client = ApiClient(configuration)
        self.email_api = TransactionalEmailsApi(self.api_client)

    def send_messages(self, email_messages):
        if not email_messages:
            return 0

        sent_count = 0

        for message in email_messages:
            try:
                to_list = [SendSmtpEmailTo(email=addr) for addr in message.to]

                # Compose sender dictionary with fallback
                sender = {}
                if message.from_email:
                    sender['email'] = message.from_email
                    if self.default_from_name:
                        sender['name'] = self.default_from_name
                elif self.default_from_email:
                    sender['email'] = self.default_from_email
                    if self.default_from_name:
                        sender['name'] = self.default_from_name
                else:
                    raise ValueError("No sender email configured: set from_email on message or DEFAULT_FROM_EMAIL in settings")

                # Handle HTML and plain text content
                html_content = None
                text_content = message.body
                if message.alternatives:
                    # alternatives is a list of tuples: (content, mimetype)
                    for content, mimetype in message.alternatives:
                        if mimetype == 'text/html':
                            html_content = content
                            break

                email = SendSmtpEmail(
                    to=to_list,
                    sender=sender,
                    subject=message.subject,
                    html_content=html_content,
                    text_content=text_content,
                )

                self.email_api.send_transac_email(email)
                sent_count += 1

            except Exception as e:
                logger.error(f"Failed to send email via Brevo: {e}", exc_info=True)
                if not self.fail_silently:
                    raise

        return sent_count

