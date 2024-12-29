from django.test import TestCase
from .anon_user_utils import create_anon_user

class AnonUserTests(TestCase):

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


