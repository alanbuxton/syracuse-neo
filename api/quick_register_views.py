from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.contrib.auth.models import User
from rest_framework.authtoken.models import Token
from allauth.account.utils import send_email_confirmation
from allauth.account.models import EmailAddress
from drf_spectacular.utils import extend_schema
from django.conf import settings # import the settings file

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
        403: {"description": "Email already registered but not verified"},
    },
    description=("Creates a user account with the given email, sends a verification email, and returns an API token. "
                f"Unverified users are limited to {settings.THROTTLES['unverified_user']} queries/month; "
                f"verified users can make {settings.THROTTLES['verified_user']}/month."
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
            send_email_confirmation(request, user)
        else:
            if not EmailAddress.objects.filter(user=user, verified=True).exists():
                send_email_confirmation(request, user)

        is_verified = EmailAddress.objects.filter(user=user, verified=True).exists()
        if is_verified is True:
            return Response({"detail": "Already registered"}, status=403)

        token, _ = Token.objects.get_or_create(user=user)
        return Response({
            "email": email,
            "token": token.key,
            "detail": ("Email verification sent. You may use the token now with limited access "
                       f"({settings.THROTTLES['unverified_user']}/month). After verifying your email, " 
                       f"the token will upgrade to the free access tier ({settings.THROTTLES['verified_user']}/month).")
        }, status=200)
