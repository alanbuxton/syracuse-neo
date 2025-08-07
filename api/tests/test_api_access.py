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
from django.core import mail
import re 
from django.test import TestCase, RequestFactory
from django.contrib.auth.models import User, AnonymousUser
from api.middleware.api_usage import APIUsageMiddleware
from api.models import APIRequestLog

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

    def test_register_existing_verified_user_sends_already_registered_email(self):
        email = "verified1@example.com"
        user = User.objects.create(username=email, email=email)
        current_mbox_size = len(mail.outbox)
        EmailAddress.objects.create(user=user, email=email, verified=True)

        _ = self.client.post(self.url, {"email": email}, format='json')
        new_mbox_size = len(mail.outbox)
        self.assertEqual(current_mbox_size + 1,  new_mbox_size)
        self.assertEqual(mail.outbox[0].subject, '[Syracuse] Email already registered')
        self.assertRegex(mail.outbox[0].body , re.compile(r"Someone.+testserver/magic_link/",
                                                          re.DOTALL))
        self.assertEqual(mail.outbox[0].from_email, settings.DEFAULT_FROM_EMAIL)

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

class APIUsageTests(APITestCase):

    def test_shows_get_api_key_button(self):
        client = self.client
        url = reverse("api-usage-list")
        email = "foobar01@example.com"
        user, _ = User.objects.get_or_create(username=email, email=email)
        token, _ = Token.objects.get_or_create(user=user)       
        client.force_login(user)
        api_call = client.get("/api/v1/geonames/")
        resp = client.get(url)
        self.assertEqual(resp.status_code,200)
        self.assertNotIn(token.key, resp.text)
        self.assertRegex(resp.text,r"<button.+Load API Token.+button>")
        self.assertIn("/api/v1/geonames", resp.text, )


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


class APIUsageMiddlewareTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.middleware = APIUsageMiddleware(get_response=self.get_response)
        self.user = User.objects.create_user(username='tester', password='pass')

    def get_response(self, request):
        # Dummy view just returns a simple response with status 200
        from django.http import HttpResponse
        return HttpResponse("OK")

    def test_middleware_logs_authenticated_user(self):
        request = self.factory.get('/api/v-test/data?industry=energy&region=eu&region=asia')
        request.user = self.user
        request.META['REMOTE_ADDR'] = '127.0.0.1'

        response = self.middleware(request)

        self.assertEqual(response.status_code, 200)

        log = APIRequestLog.objects.last()
        self.assertIsNotNone(log)
        self.assertEqual(log.user, self.user)
        self.assertEqual(log.method, 'GET')
        self.assertEqual(log.path, '/api/v-test/data')
        self.assertEqual(log.ip, '127.0.0.1')
        self.assertEqual(log.status_code, 200)

        # query_params should preserve multiple values for region
        self.assertIn('industry', log.query_params)
        self.assertEqual(log.query_params['industry'], 'energy')
        self.assertIn('region', log.query_params)
        self.assertEqual(log.query_params['region'], ['eu', 'asia'])

        self.assertTrue(log.duration >= 0)

    def test_middleware_does_not_log_anonymous_user(self):
        request = self.factory.get('/api/v-test/data')
        request.user = AnonymousUser()
        request.META['REMOTE_ADDR'] = '127.0.0.1'

        response = self.middleware(request)

        self.assertEqual(response.status_code, 200)
        self.assertFalse(APIRequestLog.objects.exists())

    def test_sensitive_query_params_are_filtered(self):
        request = self.factory.get('/api/v-test/data?token=secret_token&password=1234&foo=bar')
        request.user = self.user
        request.META['REMOTE_ADDR'] = '127.0.0.1'

        response = self.middleware(request)

        log = APIRequestLog.objects.last()
        self.assertIsNotNone(log)
        self.assertIn('foo', log.query_params)
        self.assertNotIn('token', log.query_params)
        self.assertNotIn('password', log.query_params)


class APIRequestLogQueryParamsTest(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="tester", password="pass")

        # Create some logs with different query_params
        APIRequestLog.objects.create(
            user=self.user,
            path="/api/data",
            method="GET",
            status_code=200,
            duration=0.1,
            ip="127.0.0.1",
            query_params={"region": ["eu", "asia"], "industry": "energy"},
        )
        APIRequestLog.objects.create(
            user=self.user,
            path="/api/data",
            method="GET",
            status_code=200,
            duration=0.2,
            ip="127.0.0.1",
            query_params={"region": ["us", "ca"], "industry": "tech"},
        )
        APIRequestLog.objects.create(
            user=self.user,
            path="/api/data",
            method="GET",
            status_code=200,
            duration=0.3,
            ip="127.0.0.1",
            query_params={"region": "eu", "industry": "finance"},
        )

    def test_filter_query_params_region_contains(self):
        # Find logs where query_params.region contains 'eu'
        qs = APIRequestLog.objects.filter(query_params__region__contains="eu")

        self.assertEqual(qs.count(), 2)

        # Verify the correct logs are returned
        for log in qs:
            region = log.query_params.get("region", [])
            if isinstance(region, str):
                self.assertIn("eu", region)
            else:
                self.assertIn("eu", region)