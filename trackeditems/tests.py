from django.test import TestCase
from django.http import QueryDict
import time
from django.contrib.auth import get_user_model
from rest_framework.test import APITestCase
import os
from .views import get_entities_to_track, standardize_payload
from rest_framework import status
from django.test import Client
from trackeditems.models import TrackedItem
from syracuse.cache_util import nuke_cache

'''
    Care these tests will delete neodb data
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)

class TrackedOrgIndustryGeoTestCase(TestCase):

    def test_handles_posted_update(self):
        payload = {'track_unselectall_473': ['1'], 'track_unselect_473_https://1145.am/db/3145990/Vaas_International_Holdings': ['0'], 
                   'track_unselect_473_https://1145.am/db/3451497/Health_Safety_Institute': ['0'], 'track_selectall_223': ['1'], 
                   'track_unselectall_223': ['0'], '223_https://1145.am/db/3449415/Rizing_Llc': ['1'], 
                   'track_unselect_223_https://1145.am/db/3449415/Rizing_Llc': ['0'], 'track_unselectall_DK': ['1'], 
                   'track_unselect_DK_https://1145.am/db/3474027/Gan': ['0'], 'track_selectall_US-NY': ['1'], 'track_unselectall_US-NY': ['0'], 
                   'track_US-NY_https://1145.am/db/3460219/Atria_Wealth_Solutions': ['1'], 'track_unselect_US-NY_https://1145.am/db/3460219/Atria_Wealth_Solutions': ['0'], 
                   'track_US-NY_https://1145.am/db/3474027/Gan': ['1'], 'track_unselect_US-NY_https://1145.am/db/3474027/Gan': ['0'], 
                   'track_unselectall_280_US': ['0'], 'track_unselect_280_US_https://1145.am/db/3457431/Firebirds': ['1'], 
                   'track_280_US_https://1145.am/db/3617647/Kura_Revolving_Sushi_Bar': ['1'], 'track_unselect_280_US_https://1145.am/db/3617647/Kura_Revolving_Sushi_Bar': ['0'], 
                   'track_selectall_109_US': ['1'], 'track_unselectall_109_US': ['0'], 'track_109_US_https://1145.am/db/3452658/Centre_Technologies': ['1'], 
                   'track_unselect_109_US_https://1145.am/db/3452658/Centre_Technologies': ['0'], 'track_109_US_https://1145.am/db/3474027/Gan': ['1'], 
                   'track_unselect_109_US_https://1145.am/db/3474027/Gan': ['0'], 'track_109_US_https://1145.am/db/3457048/Sphera_Solutions': ['1'], 
                   'track_unselect_109_US_https://1145.am/db/3457048/Sphera_Solutions': ['0'], 'track_109_US_https://1145.am/db/3470399/Rainfocus': ['1'], 
                   'track_unselect_109_US_https://1145.am/db/3470399/Rainfocus': ['0'], 'track_unselectall_searchstr_US': ['0'], 
                   'track_searchstr_US_https://1145.am/db/3474027/Gan': ['1'], 'track_unselect_searchstr_US_https://1145.am/db/3474027/Gan': ['0'], 
                   'track_searchstr_US_https://1145.am/db/3457038/Freightpop': ['1'], 'track_unselect_searchstr_US_https://1145.am/db/3457038/Freightpop': ['0'], 
                   'track_unselect_searchstr_US_https://1145.am/db/3452658/Centre_Technologies': ['1'], 'track_searchstr_US_https://1145.am/db/3470399/Rainfocus': ['1'], 
                   'track_unselect_searchstr_US_https://1145.am/db/3470399/Rainfocus': ['0'], 'track_searchstr_US_https://1145.am/db/3457048/Sphera_Solutions': ['1'], 
                   'track_unselect_searchstr_US_https://1145.am/db/3457048/Sphera_Solutions': ['0']}
        expected = [{'industry_id': 473, 'industry_search_str': None, 'region': None, 'organization_uri': None, 'trackable': False},
                    {'industry_id': 223, 'industry_search_str': None, 'region': None, 'organization_uri': None, 'trackable': True},
                    {'industry_id': 223, 'industry_search_str': None, 'region': 'DK', 'organization_uri': None, 'trackable': False},
                    {'industry_id': 101, 'industry_search_str': None, 'region': 'DK', 'organization_uri': None, 'trackable': False},
                    {'industry_id': 473, 'industry_search_str': None, 'region': 'US-NY', 'organization_uri': None, 'trackable': True},
                    {'industry_id': 101, 'industry_search_str': None, 'region': 'US-NY', 'organization_uri': None, 'trackable': True},
                    {'industry_id': 109, 'industry_search_str': None, 'region': 'US', 'organization_uri': None, 'trackable': True},
                    {'organization_uri': 'https://1145.am/db/3457431/Firebirds', 'trackable': False, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    {'organization_uri': 'https://1145.am/db/3617647/Kura_Revolving_Sushi_Bar', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    {'organization_uri': 'https://1145.am/db/3474027/Gan', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    {'organization_uri': 'https://1145.am/db/3457038/Freightpop', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    {'organization_uri': 'https://1145.am/db/3452658/Centre_Technologies', 'trackable': False, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    {'organization_uri': 'https://1145.am/db/3470399/Rainfocus', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    {'organization_uri': 'https://1145.am/db/3457048/Sphera_Solutions', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None},
                    ]
        tracked_items = get_entities_to_track(payload,"foobar",[473,223,101])
        assert len(tracked_items) == len(expected)
        for ti in tracked_items:
            assert ti in expected, f"Expected {ti} in {tracked_items}"

    def test_converts_industry_geo_finder_format_to_industry_geo_finder_review_format(self):
        source_dict = {'selectedIndividualCells': '["row-59#col-JM"]', 
                   'selectedRows': '["row-294"]', 
                   'selectedColumns': '["col-EG","col-MA"]', 
                   'allIndustryIDs': '[59, 749, 671, 81, 365, 74, 294, 680, 281, 207]', 
                   'searchStr': ['hospital']}
        qd = QueryDict("",mutable=True)
        qd.update(source_dict)
        res = standardize_payload(qd)
        assert res['track_selectall_294'] == '1'
        assert res['track_selectall_EG'] == '1'
        assert res['track_selectall_MA'] == '1'
        assert res['track_selectall_59_JM'] == '1'


class ToggleTrackedItemAPITest(APITestCase):
    def setUp(self):
        ts = time.time()
        self.user = get_user_model().objects.create_user(username=f"test-{ts}", password="testpass")
        self.item = TrackedItem.objects.create(user=self.user,organization_uri=f"https://www.example.org/foo/{ts}")
        self.url = f"/toggle_similar_organizations/{self.item.id}/"

    def test_authenticated_user_can_toggle_item(self):
        self.item.refresh_from_db()
        assert self.item.and_similar_orgs is False
        client = Client()
        client.force_login(self.user)
        response = client.patch(self.url)
        self.item.refresh_from_db()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["and_similar_orgs"], self.item.and_similar_orgs)
        self.assertTrue(self.item.and_similar_orgs)  # Initially False, should now be True

    def test_unauthenticated_user_cannot_toggle_item(self):
        client = Client()
        response = client.patch(self.url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_toggle_twice_restores_original_state(self):
        """Test that toggling twice returns the item to its original state."""
        # First toggle (should set and_similar_orgs to True)
        self.item.and_similar_orgs = False
        self.item.save()
        client = Client()
        client.force_login(self.user)
        response = client.patch(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.item.refresh_from_db()
        self.assertTrue(self.item.and_similar_orgs)

        # Second toggle (should revert and_similar_orgs back to False)
        response = client.patch(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.item.refresh_from_db()
        self.assertFalse(self.item.and_similar_orgs)

    def test_toggle_invalid_item_returns_404(self):
        """Test that a request for a non-existent item returns 404."""
        invalid_url = "/api/toggle-item/-1/"  # Non-existent ID
        client = Client()
        client.force_login(self.user)
        response = client.patch(invalid_url)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

class ActivityListTest(APITestCase):

    def does_not_show_site_stats_if_no_cache(self):
        '''
            see api.tests.test_with_dump_data.test_only_populates_activity_stats_if_cache_available 
            for version with cache
        '''
        client = self.client
        nuke_cache()
        response = client.get("/activity_stats")
        content = str(response.content)
        assert "Site stats calculating, please check later" in content
        assert "Showing updates as at" not in content



