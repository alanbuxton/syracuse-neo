from django.test import TestCase
from .anon_user_utils import create_anon_user
from django.conf import settings
from django.contrib.auth import get_user_model
from rest_framework.test import APIRequestFactory
from topics.models import IndustryCluster
from allauth.account.models import EmailAddress

from api.throttling import ScopedTieredThrottle

class AnonUserTests(TestCase):
    def setUp(self):
        if len(IndustryCluster.nodes) == 0:
            IndustryCluster(uri="https://example.com/foo/bar",representation=["foo","bar","baz"]).save()     # needed for anon password generation

    def test_updates_anon_user_password(self):
        u, p = create_anon_user()
        u2, p2 = create_anon_user()
        assert u.username == u2.username
        assert isinstance(p, str)
        assert p != p2
    
    def test_shows_password_on_login_form(self):
        client = self.client
        _, p = create_anon_user()
        resp = client.get("/accounts/login/")
        content = str(resp.content)
        assert p in content


User = get_user_model()

class UserThrottleLimitTests(TestCase):
    def setUp(self):
        self.factory = APIRequestFactory()

    def test_unverified_user_gets_unverified_limit(self):
        user = User.objects.create_user(username="unverified", password="pw") 

        request = self.factory.get("/api/v1/some-endpoint/")
        request.user = user

        throttle = ScopedTieredThrottle()
        throttle.allow_request(request, view=None)

        self.assertEqual(throttle.num_requests, settings.THROTTLES["unverified_user"])

    def test_verified_user_gets_verified_limit(self):
        user = User.objects.create_user(username="verified", password="pw")
        EmailAddress.objects.create(user=user, email="v@example.com", verified=True)

        request = self.factory.get("/api/v1/some-endpoint/")
        request.user = user

        throttle = ScopedTieredThrottle()
        throttle.allow_request(request, view=None)

        self.assertEqual(throttle.num_requests, settings.THROTTLES["verified_user"])

    def test_custom_limit_overrides_defaults(self):
        user = User.objects.create_user(username="custom", password="pw")
        user.userprofile.monthly_api_limit = 9999
        user.userprofile.save()
        EmailAddress.objects.create(user=user, email="c@example.com", verified=True)

        request = self.factory.get("/api/v1/some-endpoint/")
        request.user = user

        throttle = ScopedTieredThrottle()
        throttle.allow_request(request, view=None)

        self.assertEqual(throttle.num_requests, 9999)

    def test_anonymous_user_gets_minimum_limit(self):
        request = self.factory.get("/api/v1/some-endpoint/")
        request.user = type("Anon", (), {"is_authenticated": False})()

        throttle = ScopedTieredThrottle()
        throttle.allow_request(request, view=None)

        self.assertEqual(throttle.num_requests, 1)
        self.assertEqual(throttle.duration, 60 * 60 * 24 * 30)
