from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APITestCase
from rest_framework import status
from allauth.account.models import EmailAddress
from rest_framework.authtoken.models import Token
from django.contrib.auth.models import User

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