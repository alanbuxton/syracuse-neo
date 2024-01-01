from django.test import TestCase
import time
from django.contrib.auth import get_user_model
from .serializers import TrackedOrganizationModelSerializer
from .models import TrackedOrganization
from django.db.utils import IntegrityError

class TrackedOrganizationSerializerTestCase(TestCase):

    def setUp(self):
        self.ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{self.ts}")

    def test_does_not_create_duplicates(self):
        org_uri = f"https://foo.example.org/testorg2-{self.ts}"
        to1 = TrackedOrganizationModelSerializer().create({"user":self.user,"organization_uri":org_uri})
        matching_orgs = TrackedOrganization.objects.filter(user=self.user)
        assert len(matching_orgs) == 1
        assert matching_orgs[0].organization_uri == org_uri
        with self.assertRaises(IntegrityError):
            to2 = TrackedOrganizationModelSerializer().create({"user":self.user,"organization_uri":org_uri.upper()})
