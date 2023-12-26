from django.test import TestCase
import time
from django.contrib.auth import get_user_model
from userprofile.serializers import TrackedOrganizationSerializer
from userprofile.models import TrackedOrganization
from django.db.utils import IntegrityError

class TrackedOrganizationSerializerTestCase(TestCase):

    def setUp(self):
        self.ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{self.ts}")

    def test_updates_case(self):
        t1 = TrackedOrganizationSerializer()
        org_name = f"testorg1-{self.ts}"
        org_upper = org_name.upper()
        to1 = t1.create({"user":self.user,"organization_name":org_name})
        matching_orgs = TrackedOrganization.objects.filter(user=self.user)
        assert len(matching_orgs) == 1
        assert matching_orgs[0].organization_name == org_name
        to2 = t1.upsert({"user":self.user,"organization_name":org_upper})
        matching_orgs = TrackedOrganization.objects.filter(user=self.user)
        assert len(matching_orgs) == 1
        assert matching_orgs[0].organization_name == org_upper

    def test_does_not_create_duplicates(self):
        t1 = TrackedOrganizationSerializer()
        org_name = f"testorg2-{self.ts}"
        to1 = t1.create({"user":self.user,"organization_name":org_name})
        matching_orgs = TrackedOrganization.objects.filter(user=self.user)
        assert len(matching_orgs) == 1
        assert matching_orgs[0].organization_name == org_name
        with self.assertRaises(IntegrityError):
            to2 = t1.create({"user":self.user,"organization_name":org_name})
