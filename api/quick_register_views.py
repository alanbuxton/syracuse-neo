from rest_framework.views import APIView
from rest_framework.response import Response
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token
from allauth.account.utils import send_email_confirmation
from allauth.account.models import EmailAddress
from drf_spectacular.utils import extend_schema
from django.conf import settings # import the settings file
from django.core.mail import send_mail
from magic_link.models import MagicLink
import logging

logger = logging.getLogger(__name__)

@extend_schema(
    methods=["POST"],
    request={"application/json": {
            "type": "object",
            "properties": {
                "email": {"type": "string", "format": "email", "description": "User email address"},
            },
            "required": ["email"],
        }
    },
    responses={
        200: {
            "type": "object",
            "properties": {
                "email": {"type": "string"},
                "token": {"type": "string"},
                "detail": {"type": "string"}
            }
        },
        400: {"description": "Missing or invalid input"},
        403: {"description": "Email already registered"},
    },
    description=("Creates a user account with the given email, sends a verification email, and returns an API token. "
                f"Unverified users are limited to {settings.THROTTLES['unverified_user']} queries/month; "
                f"verified users can make {settings.THROTTLES['verified_user']}/month for free. "
                "Drop us an email to tell us how you found the service and if you want more queries (see website footer)"
                )
)
class RegisterAndGetKeyView(APIView):
    authentication_classes = []
    permission_classes = []

    def post(self, request):
        email = request.data.get("email")
        if not email:
            return Response({"detail": f"Email is required. You provided {request.data}"}, status=400)

        user, created = User.objects.get_or_create(username=email, defaults={"email": email})
        if created:
            send_email_confirmation_inline(request, user)
        else:
            if not EmailAddress.objects.filter(user=user, verified=True).exists():
                send_email_confirmation_inline(request, user)

        is_verified = EmailAddress.objects.filter(user=user, verified=True).exists()
        token, _ = Token.objects.get_or_create(user=user)
        if is_verified is True:
            magic_link = magic_link_for_request(request, user)
            send_already_registered_email(email, magic_link)
            return Response({"detail": "Already registered. Please check your mail for your key"}, status=403)

        return Response({
            "email": email,
            "token": token.key,
            "detail": ("Email verification sent. You may use the token now with limited access "
                       f"({settings.THROTTLES['unverified_user']}/month). After verifying your email, " 
                       f"the token will upgrade to the free access tier ({settings.THROTTLES['verified_user']}/month). ")
        }, status=200)

def magic_link_for_request(request,user):
    link = MagicLink.objects.create(user=user, redirect_to="/api/usage")
    url = request.build_absolute_uri(link.get_absolute_url())
    return url

def send_already_registered_email(email_address, magic_link):
    send_mail(
        "[Syracuse] Email already registered",
        f"Someone (hopefully you) tried to register your email account for a new key.\n\nIf it was you, and you want to review your key and usage, follow this link: {magic_link}.",
        None,
        [email_address],
        fail_silently=False,
    )

def send_email_confirmation_inline(request, user):
    try:
        logger.info(f"[Email] Starting confirmation email for user {user.pk}")
        send_email_confirmation(request, user)
        logger.info(f"[Email] Successfully sent confirmation email for user {user.pk}")
    except Exception as e:
        logger.exception(f"[Email] Failed to send confirmation email for user {user.pk}: {e}")
        raise
