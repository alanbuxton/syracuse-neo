from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from allauth.account.models import EmailAddress
from rest_framework.authtoken.models import Token
from django.contrib.auth.models import User
from django.conf import settings
from time import time
from logging import getLogger 
from django.core.cache import cache
logger = getLogger(__name__)

class CORSTest(TestCase):

    def test_cors_headers_present(self):
        response = self.client.get(
            "/api/schema/",
            HTTP_ORIGIN="http://example.com",  # Simulated cross-origin request
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Access-Control-Allow-Origin"], "*") 

class RegisterAndGetKeyViewTests(APITestCase):

    def setUp(self):
        self.url = reverse('v1:register-and-get-key')

    def test_register_new_user_returns_token(self):
        data = {"email": "newuser@example.com"}
        response = self.client.post(self.url, data, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # User created
        user = User.objects.filter(email=data["email"]).first()
        self.assertIsNotNone(user)

        # EmailAddress created but not verified
        email_addr = EmailAddress.objects.filter(user=user, email=data["email"]).first()
        self.assertIsNotNone(email_addr)
        self.assertFalse(email_addr.verified)

        # Token returned in response matches user's token
        token = Token.objects.get(user=user)
        self.assertEqual(response.data["token"], token.key)

    def test_register_existing_unverified_user_returns_token(self):
        email = "existing@example.com"
        user = User.objects.create(username=email, email=email)
        EmailAddress.objects.create(user=user, email=email, verified=False)
        token, _ = Token.objects.get_or_create(user=user)

        response = self.client.post(self.url, {"email": email}, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["token"],token.key)

    def test_register_existing_verified_user_returns_403(self):
        email = "verified@example.com"
        user = User.objects.create(username=email, email=email)
        EmailAddress.objects.create(user=user, email=email, verified=True)

        response = self.client.post(self.url, {"email": email}, format='json')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_missing_email_returns_400(self):
        response = self.client.post(self.url, {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("detail", response.data)

class TieredThrottleTests(APITestCase):

    def setUp(self):
        self.url = reverse('v1:api-region-list')
        cache.clear()

        # Unverified user
        email_u = f'unverified-{time()}@example.com'
        self.unverified_user = User.objects.create(username=email_u, email=email_u)
        self.unverified_token = Token.objects.create(user=self.unverified_user)
        EmailAddress.objects.create(user=self.unverified_user, email=self.unverified_user.email, verified=False)

        # Verified user
        email_v = f'verified-{time()}@example.com'
        self.verified_user = User.objects.create(username=email_v, email=email_v)
        self.verified_token = Token.objects.create(user=self.verified_user)
        EmailAddress.objects.create(user=self.verified_user, email=self.verified_user.email, verified=True)

    def test_throttle_for_unverified_user(self):
        client = APIClient()
        client.credentials(HTTP_AUTHORIZATION=f'Token {self.unverified_token.key}')
        for i in range(settings.THROTTLES['unverified_user']):
            response = client.get(self.url, {}, format='json')
            logger.warning(response)
            self.assertEqual(response.status_code, status.HTTP_200_OK, f"Failed on request {i+1}")

        # 11th request should be throttled
        response = client.get(self.url, {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    def test_throttle_for_verified_user(self):
        client = APIClient()
        client.credentials(HTTP_AUTHORIZATION=f'Token {self.verified_token.key}')
        for i in range(settings.THROTTLES['verified_user']):
            response = client.get(self.url, {}, format='json')
            self.assertEqual(response.status_code, status.HTTP_200_OK, f"Failed on request {i+1}")

        # 101st request should be throttled
        response = client.get(self.url, {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_429_TOO_MANY_REQUESTS)

    def test_no_throttle_for_non_api_views(self):
        client = APIClient()
        client.credentials(HTTP_AUTHORIZATION=f'Token {self.unverified_token.key}')
        for i in range(settings.THROTTLES['unverified_user']):
            response = client.get(reverse('swagger-ui'))
            self.assertEqual(response.status_code, status.HTTP_200_OK, f"Failed on request {i+1}")

        response = client.get(reverse('swagger-ui'))  
        self.assertEqual(response.status_code, status.HTTP_200_OK)