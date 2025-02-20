from django.test import TestCase
import time
from django.contrib.auth import get_user_model
from auth_extensions.anon_user_utils import create_anon_user
from trackeditems.models import TrackedItem, ActivityNotification
from rest_framework.test import APITestCase
from datetime import datetime, timezone, timedelta, date
from trackeditems.notification_helpers import (
    prepare_recent_changes_email_notification_by_max_date,
    make_email_notif_from_orgs,
    recents_by_user_min_max_date
)
from neomodel import db
from integration.models import DataImport
from topics.cache_helpers import refresh_geo_data, nuke_cache
from integration.management.commands.import_ttl import do_import_ttl
import re
import os
from integration.neo4j_utils import delete_all_not_needed_resources
from topics.models import Article, CorporateFinanceActivity
from topics.activity_helpers import activity_articles_to_api_results
from integration.rdf_post_processor import RDFPostProcessor
from .views import get_entities_to_track
from rest_framework import status
from django.test import Client
from dump.embeddings.embedding_utils import apply_latest_org_embeddings


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
                    {'industry_id': 473, 'industry_search_str': None, 'region': 'DK', 'organization_uri': None, 'trackable': False}, 
                    {'industry_id': 101, 'industry_search_str': None, 'region': 'DK', 'organization_uri': None, 'trackable': False}, 
                    {'industry_id': 101, 'industry_search_str': None, 'region': 'US-NY', 'organization_uri': None, 'trackable': True}, 
                    {'industry_id': 223, 'industry_search_str': None, 'region': 'US-NY', 'organization_uri': None, 'trackable': True}, 
                    {'industry_id': 473, 'industry_search_str': None, 'region': 'US-NY', 'organization_uri': None, 'trackable': True}, 
                    {'industry_id': 101, 'industry_search_str': None, 'region': 'US', 'organization_uri': None, 'trackable': True}, 
                    {'industry_id': 223, 'industry_search_str': None, 'region': 'US', 'organization_uri': None, 'trackable': True}, 
                    {'industry_id': 473, 'industry_search_str': None, 'region': 'US', 'organization_uri': None, 'trackable': True}, 
                    {'organization_uri': 'https://1145.am/db/3457431/Firebirds', 'trackable': False, 'industry_id': None, 'industry_search_str': None, 'region': None}, 
                    {'organization_uri': 'https://1145.am/db/3617647/Kura_Revolving_Sushi_Bar', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None}, 
                    {'organization_uri': 'https://1145.am/db/3474027/Gan', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None}, 
                    {'organization_uri': 'https://1145.am/db/3457038/Freightpop', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None}, 
                    {'organization_uri': 'https://1145.am/db/3452658/Centre_Technologies', 'trackable': False, 'industry_id': None, 'industry_search_str': None, 'region': None}, 
                    {'organization_uri': 'https://1145.am/db/3470399/Rainfocus', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None}, 
                    {'organization_uri': 'https://1145.am/db/3457048/Sphera_Solutions', 'trackable': True, 'industry_id': None, 'industry_search_str': None, 'region': None}
                    ]
        tracked_items = get_entities_to_track(payload,"foobar",[473,223,101])
        assert len(tracked_items) == len(expected)
        for ti in tracked_items:
            assert ti in expected, f"Expected {ti} in {tracked_items}"

class ActivityTestsWithSampleDataTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() is None # Empty DB
        do_import_ttl(dirname="dump",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources()
        r = RDFPostProcessor()
        r.run_all_in_order()
        refresh_geo_data()
        apply_latest_org_embeddings()

    def setUp(self):
        self.ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{self.ts}")
        # NB dates in TTL files changed to make the tests work more usefully - it's expected that the published date is later than the retrieved date
        _ = TrackedItem.objects.create(user=self.user, organization_uri="https://1145.am/db/3029576/Celgene") # "2024-03-07T18:06:00Z"
        _ = TrackedItem.objects.create(user=self.user, organization_uri="https://1145.am/db/3475299/Napajen_Pharma") # "2024-05-29T13:52:00Z"
        _ = TrackedItem.objects.create(user=self.user, organization_uri="https://1145.am/db/3458127/The_Hilb_Group") # merged from uri: https://1145.am/db/3476441/The_Hilb_Group with date datePublished: ""2024-05-27T14:05:00+00:00""
        self.ts2 = time.time()
        self.user2 = get_user_model().objects.create(username=f"test2-{self.ts2}")
        _ = TrackedItem.objects.create(user=self.user2,
                                        industry_id=146,
                                        region="US-TX")
        tracked_orgs = TrackedItem.trackable_by_user(self.user)
        org_uris = [x.organization_uri for x in tracked_orgs]
        assert set(org_uris) == {'https://1145.am/db/3029576/Celgene',
                                    'https://1145.am/db/3475299/Napajen_Pharma',
                                    'https://1145.am/db/3458127/The_Hilb_Group'}
        org_or_merged_uris = [x.organization_or_merged_uri for x in tracked_orgs]
        assert set(org_or_merged_uris) == {'https://1145.am/db/2543227/Celgene',
                                    'https://1145.am/db/3469058/Napajen_Pharma',
                                    'https://1145.am/db/3458127/The_Hilb_Group'}
        self.ts3 = time.time()
        self.user3 = get_user_model().objects.create(username=f"test3-{self.ts3}")
        _ = TrackedItem.objects.create(user=self.user3,
                                        industry_search_str="software")
        _ = TrackedItem.objects.create(user=self.user3,
                                        region = "AU")
        self.anon, _ = create_anon_user()
        self.ts4 = time.time()
        self.user4 = get_user_model().objects.create(username=f"test4-{self.ts4}")
        _ = TrackedItem.objects.create(user=self.user4, 
                                       organization_uri="https://1145.am/db/3029576/Celgene",
                                       and_similar_orgs=False)
        self.ts5 = time.time()
        self.user5 = get_user_model().objects.create(username=f"test5-{self.ts5}")
        _ = TrackedItem.objects.create(user=self.user5, 
                                       organization_uri="https://1145.am/db/3029576/Celgene",
                                       and_similar_orgs=True)
        

    def shows_tracked_organizations(self):
        client = self.client
        path = '/tracked_org_ind_geo'
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.anon)
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path)
        assert resp.status_code == 200
        content = str(resp.content)
        assert "https://1145.am/db/3029576/Celgene" in content
        # assert "<b>All Industries</b> in <b>Australia</b>" not in content
        # assert "<b>Foo bar industry</b> in <b>All Locations</b>" not in content


    def shows_tracked_industry_geos(self):
        client = self.client
        client.force_login(self.user3)
        resp = client.get('/tracked_org_ind_geo')
        content = str(resp.content)
        assert "https://1145.am/db/3029576/Celgene" not in content
        # assert "<b>All Industries</b> in <b>Australia</b>" in content
        # assert "<b>Foo bar industry</b> in <b>All Locations</b>" in content

    def shows_recent_tracked_activities(self):
        path = "/activities?max_date=2024-05-30"
        client = self.client
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.anon)
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.user3)
        resp = client.get(path)
        assert resp.status_code == 200
        content = str(resp.content)
        assert "NapaJen Pharma" not in content
        client.force_login(self.user)
        resp = client.get(path)
        content = str(resp.content)
        assert "NapaJen Pharma" in content 

    def test_creates_email_from_activity_and_article_and_write_x_more_if_many_industries(self):
        activity_uri = "https://1145.am/db/3029576/Loxo_Oncology-Acquisition"
        article_uri = "https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_"
        article = Article.self_or_ultimate_target_node(article_uri)
        activity = CorporateFinanceActivity.self_or_ultimate_target_node(activity_uri)
        activity_articles = [(activity.uri,article.uri,datetime.now(tz=timezone.utc)),]
        matching_activity_orgs = activity_articles_to_api_results(activity_articles)
        email,_ = make_email_notif_from_orgs(matching_activity_orgs,[],None,None,None)
        assert len(re.findall("Biomanufacturing Technologies, Oncology Solutions and 1 more",email)) == 1 # Per https://1145.am/db/3029576/Loxo_Oncology
        assert "Loxo Oncology" in email

    def test_creates_email_from_activity_and_shows_location_name(self):
        activity_uri = "https://1145.am/db/3029576/Loxo_Oncology-Acquisition"
        article_uri = "https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_"
        article = Article.self_or_ultimate_target_node(article_uri)
        activity = CorporateFinanceActivity.self_or_ultimate_target_node(activity_uri)
        activity_articles = [(activity.uri,article.uri,datetime.now(tz=timezone.utc)),]
        matching_activity_orgs = activity_articles_to_api_results(activity_articles)
        email,_ = make_email_notif_from_orgs(matching_activity_orgs,[],None,None,None)
        assert "Loxo Oncology" in email
        assert "<b>Region:</b> United States" in email

    def test_creates_activity_notification_for_first_time_user(self):
        ActivityNotification.objects.filter(user=self.user).delete()
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user,max_date,7)
        email, activity_notif = email_and_activity_notif
        assert len(re.findall("The Hilb Group",email)) == 3
        assert len(re.findall("Mitsui",email)) == 3
        assert "May 30, 2024" in email
        assert "May 23, 2024" in email
        assert activity_notif.num_activities == 2
        assert len(re.findall("https://web.archive.org",email)) == 4
        assert "https://www.prnewswire.com/news-releases/correction----napajen-pharma-inc-300775556.html" in email
        assert "None" not in email

    def test_creates_activity_notification_without_similar_orgs(self):
        ActivityNotification.objects.filter(user=self.user4).delete()
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user4,max_date,7)
        assert email_and_activity_notif is None

    def test_creates_activity_notification_with_similar_orgs(self):
        ActivityNotification.objects.filter(user=self.user5).delete()
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user5,max_date,7)
        email, _ = email_and_activity_notif
        assert "NapaJen" in email

    def test_creates_activity_notification_for_user_with_existing_notifications(self):
        ActivityNotification.objects.filter(user=self.user).delete()
        ActivityNotification.objects.create(user=self.user,
                max_date=datetime(2024,5,28,tzinfo=timezone.utc),num_activities=2,sent_at=datetime(2024,3,9,tzinfo=timezone.utc))
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user,max_date,7)
        assert email_and_activity_notif is not None
        email, activity_notif = email_and_activity_notif
        assert len(re.findall("The Hilb Group",email)) == 0
        assert len(re.findall("NapaJen",email)) == 4
        assert "May 30, 2024" in email
        assert "May 23, 2024" not in email
        assert "May 28, 2024" in email
        assert activity_notif.num_activities == 1

    def test_creates_geo_industry_notification_for_new_user(self):
        ActivityNotification.objects.filter(user=self.user2).delete()
        max_date = datetime(2019,1,10,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user2,max_date,7)
        email, activity_notif = email_and_activity_notif
        assert "Private Equity Business" in email
        assert "Dallas" in email
        assert activity_notif.num_activities == 2
        assert len(re.findall("Hastings",email)) == 5
        assert "None" not in email

    def test_does_not_activity_stats_if_cache_not_available(self):
        client = self.client
        nuke_cache()
        response = client.get("/activity_stats")
        content = str(response.content)
        assert "Site stats calculating, please check later" in content
        assert "Showing updates as at" not in content

    def test_only_populates_activity_stats_if_cache_available(self):
        ''' For testing
            from django.test import Client
            client = Client()
        '''
        refresh_geo_data(max_date=date(2024,5,30))
        client = self.client
        response = client.get("/activity_stats")
        content = str(response.content)
        assert "Site stats calculating, please check later" not in content
        assert "Showing updates as at" in content

    def test_always_shows_geo_activities(self):
        client = self.client
        path = "/geo_activities?geo_code=US-CA&max_date=2019-01-10"
        response = client.get(path)
        assert response.status_code == 403 
        client.force_login(self.anon)
        response = client.get(path)
        assert response.status_code == 403 
        client.force_login(self.user)
        response = client.get(path)
        assert response.status_code == 200
        content = str(response.content)
        assert "Activities between" in content
        assert "in the <b>United States of America - California</b>." in content
        assert "Site stats calculating, please check later" not in content
        assert "Pear Therapeutics raises $64M, launches prescription app for opioid use disorder" in content
        assert len(re.findall("<b>Region:</b> San Francisco",content)) == 3
        assert len(re.findall("<b>Region:</b> Ontario",content)) == 2 

    def test_always_show_source_activities(self):
        client = self.client
        path = "/source_activities?source_name=Business%20Insider&max_date=2019-01-10"
        response = client.get(path)
        assert response.status_code == 403
        client.force_login(self.anon)
        response = client.get(path)
        assert response.status_code == 403
        client.force_login(self.user)
        response = client.get(path)
        assert response.status_code == 200
        content = str(response.content)
        assert "Activities between" in content
        assert "Click on a document link to see the original source document" in content
        assert "Site stats calculating, please check later" not in content
        assert "largest banks are betting big on weed" in content

    def test_prepares_activity_data_by_org(self):
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        min_date = max_date - timedelta(days=7)
        acts, _ = recents_by_user_min_max_date(self.user,min_date,max_date)
        assert len(acts) == 2

    def test_prepares_activity_data_by_industry(self):
        max_date = datetime(2019,1,10,tzinfo=timezone.utc)
        min_date = max_date - timedelta(days=7)
        acts,_ = recents_by_user_min_max_date(self.user2,min_date,max_date)
        assert len(acts) == 2

    def test_tracked_items_updates_or_creates_no_duplicates(self):
        trackables = [{'industry_id': 401, 'industry_search_str': None, 'region': None, 'organization_uri': None, 'trackable': True}, 
                      {'industry_id': None, 'industry_search_str': None, 'region': 'US-TX', 'organization_uri': None, 'trackable': True}]
        ts = time.time()
        user = get_user_model().objects.create(username=f"test-{ts}")
        tracked_items = TrackedItem.trackable_by_user(user)
        assert len(tracked_items) == 0
        TrackedItem.update_or_create_for_user(user, trackables)
        tracked_items = TrackedItem.trackable_by_user(user)
        assert len(tracked_items) == 2
        trackables = [{'industry_id': 400, 'industry_search_str': None, 'region': None, 'organization_uri': None, 'trackable': True},  # New one
                      {'industry_id': None, 'industry_search_str': None, 'region': 'US-TX', 'organization_uri': None, 'trackable': True}] # Same already there
        TrackedItem.update_or_create_for_user(user, trackables)
        tracked_items = TrackedItem.trackable_by_user(user)
        assert len(tracked_items) == 3

    def test_tracked_items_applies_not_trackable(self):
        trackables = [{'industry_id': 401, 'industry_search_str': None, 'region': None, 'organization_uri': None, 'trackable': True}, 
                      {'industry_id': None, 'industry_search_str': None, 'region': 'US-TX', 'organization_uri': None, 'trackable': False}]
        ts = time.time()
        user = get_user_model().objects.create(username=f"test-{ts}")
        tracked_items = TrackedItem.trackable_by_user(user)
        assert len(tracked_items) == 0
        TrackedItem.update_or_create_for_user(user, trackables)
        tracked_items = TrackedItem.trackable_by_user(user)
        assert len(tracked_items) == 1 # One of the items is not trackable

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
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

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



