from django.test import TestCase
import time
from django.contrib.auth import get_user_model
from trackeditems.serializers import TrackedOrganizationModelSerializer
from trackeditems.models import TrackedOrganization, ActivityNotification, TrackedIndustryGeo
from django.db.utils import IntegrityError
from datetime import datetime, timezone, timedelta
from trackeditems.notification_helpers import (
    prepare_recent_changes_email_notification_by_max_date,
    make_email_notif_from_orgs,
    recents_by_user_min_max_date
)
from neomodel import db
from integration.models import DataImport
from topics.precalculator_helpers import warm_up_precalculator
from precalculator.models import P
from integration.management.commands.import_ttl import do_import_ttl
import re
import os
from integration.neo4j_utils import delete_all_not_needed_resources
from topics.models import Article, CorporateFinanceActivity
from topics.model_queries import activity_articles_to_api_results
from integration.rdf_post_processor import RDFPostProcessor

'''
    Care these tests will delete neodb data
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)


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

class ActivityTestsWithSampleDataTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() is None # Empty DB
        P.nuke_all()
        do_import_ttl(dirname="dump",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources()
        r = RDFPostProcessor()
        r.run_all_in_order()
        warm_up_precalculator()

    def setUp(self):
        self.ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{self.ts}")
        # NB dates in TTL files changed to make the tests work more usefully - it's expected that the published date is later than the retrieved date
        to1 = TrackedOrganization.objects.create(user=self.user, organization_uri="https://1145.am/db/3029576/Celgene") # "2024-03-07T18:06:00Z"
        to2 = TrackedOrganization.objects.create(user=self.user, organization_uri="https://1145.am/db/3475299/Napajen_Pharma") # "2024-05-29T13:52:00Z"
        to3 = TrackedOrganization.objects.create(user=self.user, organization_uri="https://1145.am/db/3458127/The_Hilb_Group") # merged from uri: https://1145.am/db/3476441/The_Hilb_Group with date datePublished: ""2024-05-27T14:05:00+00:00""
        self.ts2 = time.time()
        self.user2 = get_user_model().objects.create(username=f"test-{self.ts2}")
        tig1 = TrackedIndustryGeo.objects.create(user=self.user2,
                                        industry_name="Financial Planning And Wealth Management Services",
                                        geo_code="US-TX")

        tracked_orgs = TrackedOrganization.by_user(self.user)
        org_uris = [x.organization_uri for x in tracked_orgs]
        assert set(org_uris) == {'https://1145.am/db/3029576/Celgene',
                                    'https://1145.am/db/3475299/Napajen_Pharma',
                                    'https://1145.am/db/3458127/The_Hilb_Group'}
        org_or_merged_uris = [x.organization_or_merged_uri for x in tracked_orgs]
        assert set(org_or_merged_uris) == {'https://1145.am/db/2543227/Celgene',
                                    'https://1145.am/db/3469058/Napajen_Pharma',
                                    'https://1145.am/db/3458127/The_Hilb_Group'}


    def test_creates_email_from_activity_and_article_and_write_x_more_if_many_industries(self):
        activity_uri = "https://1145.am/db/3029576/Loxo_Oncology-Acquisition"
        article_uri = "https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_"
        article = Article.self_or_ultimate_target_node(article_uri)
        activity = CorporateFinanceActivity.self_or_ultimate_target_node(activity_uri)
        activity_articles = [(activity,article),]
        matching_activity_orgs = activity_articles_to_api_results(activity_articles)
        email,_ = make_email_notif_from_orgs(matching_activity_orgs,[],[],None,None,None)
        assert len(re.findall("Biomanufacturing Technologies, Oncology Solutions and 1 more",email)) == 1 # Per https://1145.am/db/3029576/Loxo_Oncology
        assert "Loxo Oncology" in email

    def test_creates_email_from_activity_and_shows_location_name(self):
        activity_uri = "https://1145.am/db/3029576/Loxo_Oncology-Acquisition"
        article_uri = "https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_"
        article = Article.self_or_ultimate_target_node(article_uri)
        activity = CorporateFinanceActivity.self_or_ultimate_target_node(activity_uri)
        activity_articles = [(activity,article),]
        matching_activity_orgs = activity_articles_to_api_results(activity_articles)
        email,_ = make_email_notif_from_orgs(matching_activity_orgs,[],[],None,None,None)
        assert "Loxo Oncology" in email
        assert "<b>Region:</b> United States" in email

    def test_creates_activity_notification_for_first_time_user(self):
        ActivityNotification.objects.filter(user=self.user).delete()
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user,max_date,7)
        email, activity_notif = email_and_activity_notif
        assert len(re.findall("The Hilb Group",email)) == 4
        assert len(re.findall("Mitsui",email)) == 3 # Once in activity URL, twice in body
        assert "May 30, 2024" in email
        assert "May 23, 2024" in email
        assert activity_notif.num_activities == 2
        assert len(re.findall("https://web.archive.org",email)) == 4
        assert "https://www.prnewswire.com/news-releases/correction----napajen-pharma-inc-300775556.html" in email
        assert "None" not in email

    def test_creates_activity_notification_for_user_with_existing_notifications(self):
        ActivityNotification.objects.filter(user=self.user).delete()
        ActivityNotification.objects.create(user=self.user,
                max_date=datetime(2024,5,28,tzinfo=timezone.utc),num_activities=2,sent_at=datetime(2024,3,9,tzinfo=timezone.utc))
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user,max_date,7)
        assert email_and_activity_notif is not None
        email, activity_notif = email_and_activity_notif
        assert len(re.findall("The Hilb Group",email)) == 1
        assert len(re.findall("NapaJen",email)) == 5
        assert "May 30, 2024" in email
        assert "May 23, 2024" not in email
        assert "May 28, 2024" in email
        assert activity_notif.num_activities == 1

    def test_creates_geo_industry_notification_for_new_user(self):
        ActivityNotification.objects.filter(user=self.user2).delete()
        max_date = datetime(2019,1,10,tzinfo=timezone.utc)
        email_and_activity_notif = prepare_recent_changes_email_notification_by_max_date(self.user2,max_date,7)
        email, activity_notif = email_and_activity_notif
        assert "<b>Financial Planning And Wealth Management Services</b> in the <b>United States - Texas</b>" in email
        assert "We are not tracking any specific organizations for you." in email
        assert activity_notif.num_activities == 2
        assert len(re.findall("Atria Wealth Solutions",email)) == 5
        assert "None" not in email

    def test_does_not_activity_stats_if_cache_not_available(self):
        client = self.client
        P.nuke_all()

        response = client.get("/activity_stats")
        content = str(response.content)
        assert "Site stats calculating, please check later" in content
        assert "Showing updates as at" not in content

    def test_only_populates_activity_stats_if_cache_available(self):
        ''' For testing
            from django.test import Client
            client = Client()
        '''
        P.nuke_all()
        warm_up_precalculator()
        client = self.client

        response = client.get("/activity_stats")
        content = str(response.content)
        assert "Site stats calculating, please check later" not in content
        assert "Showing updates as at" in content

    def test_always_shows_geo_activities(self):
        client = self.client
        response = client.get("/geo_activities?geo_code=US-CA&max_date=2019-01-10")
        content = str(response.content)
        assert "Activities between" in content
        assert "Site stats calculating, please check later" not in content
        assert "MUFG Union Bank Completes the Acquisition of Intrepid Investment Bankers" in content

        response = client.get("/source_activities?source_name=Business%20Insider&max_date=2019-01-10")
        content = str(response.content)
        assert "Activities between" in content
        assert "Click on a document link to see the original source document" in content
        assert "Site stats calculating, please check later" not in content
        assert "largest banks are betting big on weed" in content

    def test_prepares_activity_data_by_org(self):
        max_date = datetime(2024,5,30,tzinfo=timezone.utc)
        min_date = max_date - timedelta(days=7)
        acts, _, _ = recents_by_user_min_max_date(self.user,min_date,max_date)
        assert len(acts) == 2

    def test_prepares_activity_data_by_industry(self):
        max_date = datetime(2019,1,10,tzinfo=timezone.utc)
        min_date = max_date - timedelta(days=7)
        acts,_,_ = recents_by_user_min_max_date(self.user2,min_date,max_date)
        assert len(acts) == 2



