from django.test import TestCase
from collections import OrderedDict
from topics.models import *
from topics.stats_helpers import get_stats, date_minus
from auth_extensions.anon_user_utils import create_anon_user
from topics.activity_helpers import (get_activities_by_country_and_date_range, activities_by_industry, 
            activities_by_region, get_activities_by_industry_geo_and_date_range,
            get_activities_by_org_and_date_range,
            )
from topics.graph_utils import graph_centered_on
from topics.timeline_utils import get_timeline_data
import os
from integration.management.commands.import_ttl import do_import_ttl
from integration.models import DataImport
from neomodel import db
from datetime import date, datetime, timezone, timedelta
import time
from django.contrib.auth import get_user_model
from topics.serializers import *
from integration.neo4j_utils import delete_all_not_needed_resources
from integration.rdf_post_processor import RDFPostProcessor
from topics.organization_search_helpers import get_same_as_name_onlies, get_by_internal_clean_name
import json
import re
from topics.serializers import (FamilyTreeSerializer, 
    create_min_date_pretty_print_data, orgs_by_connection_count,
)
from topics.industry_geo.orgs_by_industry_geo import combined_industry_geo_results
from topics.cache_helpers import refresh_geo_data, nuke_cache
from topics.industry_geo import org_uris_by_industry_and_or_geo, geo_codes_for_region, GEO_PARENT_CHILDREN
from topics.views import remove_not_needed_admin1s_from_individual_cells
from topics.models.model_helpers import similar_organizations
from dump.embeddings.embedding_utils import apply_latest_org_embeddings
from trackeditems.notification_helpers import (
    prepare_recent_changes_email_notification_by_max_date,
    make_email_notif_from_orgs,
    recents_by_user_min_max_date
)
from trackeditems.models import TrackedItem, ActivityNotification
from topics.models import Article, CorporateFinanceActivity
from topics.activity_helpers import activity_articles_to_api_results


'''
    Care these tests will delete neodb data
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)


class EndToEndTests(TestCase):

    def setUpTestData():
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() == None # Empty DB
        do_import_ttl(dirname="dump",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources()
        r = RDFPostProcessor()
        r.run_all_in_order()
        refresh_geo_data(max_date=date(2024,6,2))
        apply_latest_org_embeddings()

    def setUp(self):
        ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{ts}")
        self.anon, _ = create_anon_user()
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

    def test_adds_model_classes_with_multiple_labels(self):
        uri = "https://1145.am/db/2858242/Search_For_New_Chief"
        res = Resource.nodes.get_or_none(uri=uri)
        assert res.uri == uri
        assert res.__class_name_is_label__ == False 
        assert res.whereHighClean == ['South Africa']

    def test_data_list_choice_field_include_great_britain_option(self):
        geo = GeoSerializer()   
        field = geo.fields['country_or_region']
        assert 'United Kingdom of Great Britain and Northern Ireland' in field.choices.keys(), "Great Britain not in field choices"
        assert 'United Kingdom of Great Britain and Northern Ireland' in field.choices.values(), "Great Britain not in field choices"

    def test_returns_great_britain_gb(self):
        geo = GeoSerializer(data={"country_or_region":"United Kingdom of Great Britain and Northern Ireland"})
        assert geo.get_country_or_region_id() == 'GB'

    def test_corp_fin_graph_nodes(self):
        source_uri = "https://1145.am/db/3558745/Jb_Hunt"
        o = Organization.self_or_ultimate_target_node(source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o)
        assert len(clean_node_data) == 6
        assert set([x['label'] for x in clean_node_data]) == set(
            ['Acquisition (Business Insider: Jan 2019)',
            'Buying furniture from the internet has become normal - and trucking companies are investing millions in the e-commerce boom',
            'Cory 1st Choice Home Delivery', 'J.B. Hunt', 'United States',
            'Truckload Freight Services']
        )
        assert len(clean_edge_data) == 7
        assert set([x['label'] for x in clean_edge_data]) == {'industryClusterPrimary', 'buyer', 'basedInHighGeoNamesLocation', 'whereHighGeoNamesLocation', 'documentSource', 'target'}
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_graph_nodes_max_nodes(self):
        source_uri = "https://1145.am/db/3558745/Jb_Hunt"
        o = Organization.self_or_ultimate_target_node(source_uri)
        res = graph_centered_on(o,max_nodes=5)
        assert res is None

    def test_corp_fin_timeline(self):
        source_uri = "https://1145.am/db/3558745/Jb_Hunt"
        o = Organization.self_or_ultimate_target_node(source_uri)
        groups, items, item_display_details, org_display_details = get_timeline_data(o,True,Article.all_sources())
        assert len(groups) == 5
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1

    def test_location_graph(self):
        source_uri = "https://1145.am/db/1736082/Tesla"
        o = Organization.self_or_ultimate_target_node(source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o,
                                                            source_names=Article.all_sources())
        clean_uris = set([x['id'] for x in clean_node_data]) 
        expected = set(['https://1145.am/db/1736082/Berlin', 'https://1145.am/db/1736082/Brandenburg', 'https://1145.am/db/1736082/Tesla-Added-Berlin',
                                    'https://1145.am/db/1736082/Gr_Enheide', 'https://1145.am/db/1736082/Tesla', 
                                    'https://1145.am/db/1736082/techcrunchcom_2019_12_21_tesla-nears-land-deal-for-german-gigafactory-outside-of-berlin_', 
                                    'https://1145.am/db/geonames_location/2921044', 'https://1145.am/db/geonames_location/2945356', 'https://1145.am/db/geonames_location/2950159', 
                                    'https://1145.am/db/geonames_location/553898', 'https://1145.am/db/industry/302_automakers_carmakers_automaker_automaking'])
        assert clean_uris == expected, f"Got {clean_uris} - diff = {clean_uris.symmetric_difference(expected)}"
        assert len(clean_edge_data) == 16
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_timeline(self):
        source_uri = "https://1145.am/db/1736082/Tesla"
        o = Organization.self_or_ultimate_target_node(source_uri)
        groups, items, item_display_details, org_display_details = get_timeline_data(o,True,Article.all_sources())
        assert len(groups) == 5
        assert len(items) == 3
        assert set([x['label'] for x in items]) == {'Added Berlin', 'Added Brandenburg', 'Added Grüenheide'}
        assert len(org_display_details) == 1

    def test_role_graph(self):
        source_uri = "https://1145.am/db/1824114/Square"
        o = Organization.self_or_ultimate_target_node(source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o,
                                                            source_names=Article.all_sources())
        assert len(clean_node_data) == 7
        assert len(clean_edge_data) == 9
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_role_timeline(self):
        source_uri = "https://1145.am/db/1824114/Square"
        o = Organization.self_or_ultimate_target_node(source_uri)
        groups, items, item_display_details, org_display_details = get_timeline_data(o,True,
                                                            source_names=Article.all_sources())
        assert len(groups) == 5
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1

    def test_track_org_button_only_appears_if_logged_in_as_real_user(self):
        path = "/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=1&sources=_all&min_date=-1"
        client = self.client
        response = client.get(path)
        assert response.status_code == 200
        content = str(response.content)
        assert "Track Discovery, Inc" not in content
        client.force_login(self.anon)
        response = client.get(path)
        assert response.status_code == 200
        content = str(response.content)
        assert "Track Discovery, Inc" not in content
        client.force_login(self.user)
        response = client.get(path)
        assert response.status_code == 200
        content = str(response.content)
        assert "Track Discovery, Inc" in content


    def test_organization_graph_view_with_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=1&sources=_all&min_date=-1")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 114
        assert "technologies" in content # from sameAsNameOnly's industry

    def test_organization_graph_view_without_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=0&sources=_all&min_date=-1")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 50
        assert "technologies" not in content

    def test_stats(self):
        max_date = date.fromisoformat("2024-06-02")
        counts, recents_by_geo, recents_by_source, recents_by_industry = get_stats(max_date)
        expected = {('AboutUs', 1), ('Person', 12), ('OperationsActivity', 4), ('IncidentActivity', 1), 
                                ('RecognitionActivity', 1), ('EquityActionsActivity', 2), ('PartnershipActivity', 4), 
                                ('ProductActivity', 10), ('RegulatoryActivity', 1), ('FinancialReportingActivity', 1),
                                ('MarketingActivity', 3), ('FinancialsActivity', 2), ('Organization', 434), ('Article', 213), 
                                ('CorporateFinanceActivity', 189), 
                                ('AnalystRatingActivity', 1), ('LocationActivity', 7), ('Role', 11), ('RoleActivity', 12)}

        counts_set = set(counts)
        assert counts_set == expected, f"Got {counts_set} - diff = {counts_set.symmetric_difference(expected)}"
        assert sorted(recents_by_geo) == [('AU', 'Australia', 1, 1, 1), ('CA', 'Canada', 3, 3, 3), ('CN', 'China', 2, 2, 2), 
                                            ('CZ', 'Czechia', 1, 1, 1), ('DK', 'Denmark', 1, 1, 1), ('EG', 'Egypt', 0, 0, 1), 
                                            ('ES', 'Spain', 1, 1, 1), ('GB', 'United Kingdom of Great Britain and Northern Ireland', 3, 3, 3), 
                                            ('IE', 'Ireland', 1, 1, 1), ('IL', 'Israel', 1, 1, 1), ('IT', 'Italy', 1, 1, 1), ('JP', 'Japan', 0, 0, 1), 
                                            ('KE', 'Kenya', 1, 1, 1), ('UG', 'Uganda', 1, 1, 1), ('US', 'United States of America', 16, 16, 37)]

        assert sorted(recents_by_source) == [('Associated Press', 3, 3, 3), ('Business Insider', 2, 2, 2), ('Business Wire', 1, 1, 1), 
                                                ('CityAM', 1, 1, 4), ('Fierce Pharma', 0, 0, 3), ('GlobeNewswire', 2, 2, 2), 
                                                ('Hotel Management', 0, 0, 1), ('Live Design Online', 0, 0, 1), ('MarketWatch', 3, 3, 3), 
                                                ('PR Newswire', 20, 20, 33), ('Reuters', 1, 1, 1), ('TechCrunch', 0, 0, 1), 
                                                ('The Globe and Mail', 1, 1, 1), ('VentureBeat', 0, 0, 1)]
        assert recents_by_industry[:10] == [(696, 'Architectural And Design', 0, 0, 1), (154, 'Biomanufacturing Technologies', 0, 0, 3), 
                                            (26, 'Biopharmaceutical And Biotech Industry', 1, 1, 6), (36, 'C-Commerce (\\', 1, 1, 1), 
                                            (12, 'Cannabis And Hemp', 1, 1, 1), (236, 'Chemical And Technology', 0, 0, 1), (74, 'Chip Business', 1, 1, 1), 
                                            (4, 'Cloud Services', 0, 0, 1), (165, 'Development Banks', 1, 1, 1), 
                                            (134, 'Electronic Manufacturing Services And Printed Circuit Board Assembly', 1, 1, 1)]

    def test_recent_activies_by_industry(self):
        max_date = date.fromisoformat("2024-06-02")
        sample_ind = IndustryCluster.nodes.get_or_none(topicId=154)
        res = activities_by_industry(sample_ind,date_minus(max_date,90),max_date,counts_only=False)
        assert len(res) == 3
        assert ['https://1145.am/db/3029576/Tesaro-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)] in res
        assert ['https://1145.am/db/3029576/Loxo_Oncology-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)] in res 
        assert ['https://1145.am/db/3029576/Celgene-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)] in res

    def test_recent_activities_by_region(self):
        max_date = date.fromisoformat("2024-06-02")
        sample_acts = activities_by_region("AU",date_minus(max_date,7),max_date,counts_only=False)
        assert sample_acts == [['https://1145.am/db/4290457/Gyg-Ipo', 
                                'https://1145.am/db/4290457/wwwreuterscom_markets_deals_australian-fast-food-chain-guzman-y-gomez-seeks-raise-161-mln-june-ipo-2024-05-31_', 
                                datetime(2024, 5, 31, 5, 12, 37, 320000, tzinfo=timezone.utc)]]
        sample_acts = activities_by_region("GB",date_minus(max_date,7),max_date,counts_only=False)
        assert len(sample_acts) == 3
        assert ['https://1145.am/db/4290472/Associated_British_Foods-Ipo', 'https://1145.am/db/4290472/wwwmarketwatchcom_story_ab-foods-majority-shareholder-sells-10-3-mln-shares-for-gbp262-mln-067222fe', datetime(2024, 5, 31, 6, 42, tzinfo=timezone.utc)] in sample_acts
        assert ['https://1145.am/db/3474027/Aquiline_Technology_Growth-Gan-Investment-Series_B', 'https://1145.am/db/3474027/wwwprnewswirecom_news-releases_gan-integrity-raises-15-million-to-accelerate-global-compliance-solution-300775390html', datetime(2024, 5, 29, 13, 15, tzinfo=timezone.utc)] in sample_acts
        assert ['https://1145.am/db/3473030/Sylvant-Acquisition-Rights', 'https://1145.am/db/3473030/wwwprnewswirecom_news-releases_eusa-pharma-completes-acquisition-of-global-rights-to-sylvant-siltuximab--and-presents-company-update-at-37th-jp-morgan-healthcare-conference-300775508html', datetime(2024, 5, 29, 13, 0, tzinfo=timezone.utc)] in sample_acts


    def test_recent_activities_by_country(self):
        max_date = date.fromisoformat("2024-06-02")
        min_date = date.fromisoformat("2024-05-03")
        country_code = 'US-NY'
        matching_activity_orgs = get_activities_by_country_and_date_range(country_code,min_date,max_date,limit=20)
        assert len(matching_activity_orgs) == 5
        sorted_actors = [tuple(sorted(x['actors'].keys())) for x in matching_activity_orgs]
        assert set(sorted_actors) == {('participant', 'protagonist'), ('buyer', 'target'), ('investor', 'target')}
        activity_classes = sorted([x['activity_class'] for x in matching_activity_orgs])
        assert Counter(activity_classes).most_common() == [('CorporateFinanceActivity', 5)]
        uris = sorted([x['activity_uri'] for x in matching_activity_orgs])
        assert uris == ['https://1145.am/db/3472994/Ethos_Veterinary_Health_Llc-Investment', 
                        'https://1145.am/db/3474027/Aquiline_Technology_Growth-Gan-Investment-Series_B', 
                        'https://1145.am/db/3475220/Novel_Bellevue-Investment', 
                        'https://1145.am/db/4290170/Abbvie_Inc-Bleichmar_Fonti_Auld_Llp-Cerevel_Therapeutics_Holdings_Inc-Merger', 
                        'https://1145.am/db/4290170/Cerevel_Therapeutics_Holdings_Inc-Acquisition']

    def test_search_by_industry_and_geo(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = "Biopharmaceutical And Biotech Industry"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None
        org_data = org_uris_by_industry_and_or_geo(industry,selected_geo)
        assert org_data == [('https://1145.am/db/2364647/Mersana_Therapeutics', 5), 
                           ('https://1145.am/db/3473030/Eusa_Pharma', 4)]

    def test_search_by_industry_only(self):
        selected_geo_name = ""
        industry_name = "Biopharmaceutical And Biotech Industry"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None
        org_data = org_uris_by_industry_and_or_geo(industry,selected_geo)
        expected = [('https://1145.am/db/2543227/Celgene', 12), ('https://1145.am/db/2364647/Mersana_Therapeutics', 5), 
                            ('https://1145.am/db/2364624/Parexel_International_Corporation', 4), ('https://1145.am/db/3473030/Eusa_Pharma', 4), 
                            ('https://1145.am/db/3473030/Janssen_Sciences_Ireland_Uc', 3), ('https://1145.am/db/3473030/Sylvant', 3)]
        assert org_data == expected, f"Got {org_data}, expected {expected}"

    def test_search_by_geo_only(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = ""
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is None
        org_data = org_uris_by_industry_and_or_geo(industry,selected_geo)
        expected = [('https://1145.am/db/2364647/Mersana_Therapeutics', 5), ('https://1145.am/db/2946625/Cuadrilla_Resources', 5), 
                    ('https://1145.am/db/1787315/Scape', 4), ('https://1145.am/db/3029681/Halebury', 4), 
                    ('https://1145.am/db/3452608/Avon_Products_Inc', 4), ('https://1145.am/db/3465815/Alliance_Automotive_Group', 4), 
                    ('https://1145.am/db/3465883/Pistonheads', 4), ('https://1145.am/db/3473030/Eusa_Pharma', 4)]
        assert org_data == expected, f"Got {org_data}, expected {expected}"

    def test_shows_resource_page(self):
        client = self.client
        path = "/resource/1145.am/db/3544275/wwwbusinessinsidercom_hotel-zena-rbg-mural-female-women-hotel-travel-washington-dc-2019-12"
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path)
        assert resp.status_code == 200
        content = str(resp.content)
        assert "https://www.businessinsider.com/hotel-zena-rbg-mural-female-women-hotel-travel-washington-dc-2019-12" in content
        assert "first female empowerment-themed hotel will open in Washington, DC with a Ruth Bader Ginsburg mural" in content
        assert "<strong>Document Url</strong>" in content
        assert "<strong>Headline</strong>" in content
        assert "<strong>Name</strong>" not in content

    def test_shows_direct_parent_child_rels(self):
        client = self.client
        path = "/organization/family-tree/uri/1145.am/db/3451381/Responsability_Investments_Ag?combine_same_as_name_only=0&rels=buyer,investor,vendor&min_date=-1"
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path)
        assert resp.status_code == 200
        content = str(resp.content)
        assert "REDAVIA" in content
        assert "REDOVIA" not in content

    def test_shows_parent_child_rels_via_same_as_name_only(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/3451381/Responsability_Investments_Ag?combine_same_as_name_only=1&rels=buyer,investor,vendor&min_date=-1")
        content = str(resp.content)
        assert "REDAVIA" in content
        assert "REDOVIA" in content

    def test_company_search_with_combine_same_as_name_only(self):
        client = self.client
        resp = client.get("/?name=eli&combine_same_as_name_only=1&min_date=-1")
        content = str(resp.content)
        assert "Eli Lilly" in content
        assert "https://1145.am/db/3029576/Eli_Lilly" in content
        assert "Eli Lilly and Company" not in content
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" not in content

    def test_company_search_without_combine_same_as_name_only(self):
        client = self.client
        resp = client.get("/?name=eli&combine_same_as_name_only=0&min_date=-1")
        content = str(resp.content)
        assert "Eli Lilly" in content
        assert "https://1145.am/db/3029576/Eli_Lilly" in content
        assert "Eli Lilly and Company" in content
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" in content

    def test_same_as_search_with_two_words(self):
        uri = "https://1145.am/db/3029576/Eli_Lilly"
        o = Organization.self_or_ultimate_target_node(uri)
        res = get_same_as_name_onlies(o)
        assert len(res) == 1
        assert res.pop().uri == 'https://1145.am/db/3448439/Eli_Lilly_And_Company'

    def test_same_as_search_with_one_word1(self):
        term1 = "loxo"
        res1 = get_by_internal_clean_name(term1)
        assert len(res1) == 1
        assert list(res1.keys())[0].uri == 'https://1145.am/db/3029576/Loxo_Oncology'
        
    def test_same_as_search_with_one_word2(self):
        term2 = "loxo oncology"
        res2 = get_by_internal_clean_name(term2)
        assert len(res2) == 2
        assert [(x.uri,y) for x,y in res2.items()] == [('https://1145.am/db/3029576/Loxo_Oncology', 10), 
                                                       ('https://1145.am/db/3464715/Loxo_Oncology', 4)]

    def test_same_as_search_with_one_word3(self):
        term3 = "loxo oncology two"
        res3 = get_by_internal_clean_name(term3)
        assert len(res3) == 1
        assert list(res3.keys())[0].uri == 'https://1145.am/db/3448439/Loxo_Oncology2'

    def test_graph_combines_same_as_name_only_off_vs_on_based_on_target_node(self):
        client = self.client
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0&min_date=-1")
        content0 = str(resp.content)
        inds0 = len(re.findall("IndustryCluster",content0))
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=1&min_date=-1")
        content1 = str(resp.content)
        inds1 = len(re.findall("IndustryCluster",content1))
        assert "Drug R & D/Manufacturing" not in content0
        assert "Drug R & D/Manufacturing" in content1 # comes from https://1145.am/db/3464715/Loxo_Oncology 
        assert inds1 > inds0 

    def test_graph_combines_same_as_name_only_off_vs_on_based_on_central_node(self):
        client = self.client
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=0&min_date=-1&sources=_all")
        content0 = str(resp.content)
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=1&min_date=-1&sources=_all")
        content1 = str(resp.content)
        assert "https://1145.am/db/3464715/Loxo_Oncology-Acquisition" in content0
        assert "https://1145.am/db/3464715/Loxo_Oncology-Acquisition" in content1
        assert "https://1145.am/db/3448439/Loxo_Oncology2-Acquisition" not in content0
        assert "https://1145.am/db/3448439/Loxo_Oncology2-Acquisition" in content1

    def test_timeline_combines_same_as_name_only_on_off(self):
        client = self.client
        path0 = "/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=0&min_date=-1&sources=_all"
        resp = client.get(path0)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path0)
        assert resp.status_code == 200
        content0 = str(resp.content)
        resp = client.get("/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=1&min_date=-1&sources=_all")
        content1 = str(resp.content)
        assert "https://1145.am/db/3549221/Loxo_Oncology-Acquisition" in content0
        assert "https://1145.am/db/3549221/Loxo_Oncology-Acquisition" in content1
        assert "https://1145.am/db/3448439/Loxo_Oncology2-Acquisition" not in content0
        assert "https://1145.am/db/3448439/Loxo_Oncology2-Acquisition" in content1

    def test_family_tree_only_available_if_logged_in(self):
        client = self.client
        path0 = "/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0&sources=_all&min_date=-1"
        resp = client.get(path0)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path0)
        assert resp.status_code == 200

    def test_family_tree_combine_same_as_name_only_off(self):
        client = self.client
        path = "/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0&sources=_all&min_date=-1"
        client.force_login(self.user)
        resp = client.get(path)
        content0 = str(resp.content)
        assert "https://1145.am/db/3029576/Eli_Lilly" in content0
        assert "https://1145.am/db/3029576/Loxo_Oncology" in content0 # two orgs with no sameAsHigh relationship
        assert "https://1145.am/db/3464715/Loxo_Oncology" in content0 
        assert "Buyer (CityAM Mar 2024)" in content0
        assert "Buyer (PR Newswire Jan 2019)" in content0
        assert "Buyer (PR Newswire - FOO TEST Jan 2019)" not in content0

    def test_family_tree_combine_same_as_name_only_on(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=1&sources=_all&min_date=-1")
        content1 = str(resp.content)

        assert "https://1145.am/db/3029576/Eli_Lilly" in content1
        assert "https://1145.am/db/3029576/Loxo_Oncology" in content1 # merged the 3029576/346715
        assert "https://1145.am/db/3448439/Loxo_Oncology2" in content1 # matches name
        assert "Buyer (CityAM Mar 2024)" in content1
        assert "Buyer (PR Newswire Jan 2019)" not in content1
        assert "Buyer (PR Newswire - FOO TEST Jan 2019)" in content1

    def test_family_tree_serializer_sorts_edges(self):
        uri = "https://1145.am/db/2543227/Celgene"
        o = Organization.self_or_ultimate_target_node(uri)
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":True,
                                                                "relationship_str":"buyer,vendor",
                                                                "source_str":"_all"})
        vals = nodes_edges.data
        assert vals["edges"] == [{'id': 'https://1145.am/db/2543227/Bristol-Myers-buyer-https://1145.am/db/2543227/Celgene', 
                                    'from': 'https://1145.am/db/2543227/Bristol-Myers', 'to': 'https://1145.am/db/2543227/Celgene',
                                    'color': 'blue', 'label': 'Buyer (Fierce Pharma Mar 2024)', 'arrows': 'to'}, 
                                 {'id': 'https://1145.am/db/3029576/Bristol-Myers_Squibb-buyer-https://1145.am/db/2543227/Celgene', 
                                    'from': 'https://1145.am/db/3029576/Bristol-Myers_Squibb', 'to': 'https://1145.am/db/2543227/Celgene', 
                                    'color': 'blue', 'label': 'Buyer (CityAM Mar 2024)', 'arrows': 'to'}]

    def test_family_tree_uris_acquisition(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor&combine_same_as_name_only=0&sources=_all&min_date=-1")
        content = str(resp.content)
        assert "Faroe Petroleum" in content
        assert "Bowery Valuation" not in content
        assert re.search(r"Family tree relationships:<\/strong>\\n\s*Acquisitions",content) is not None
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=investor&combine_same_as_name_only=0&sources=_all&min_date=-1">Investments</a>""" in content
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cinvestor%2Cvendor&combine_same_as_name_only=0&sources=_all&min_date=-1">All</a>""" in content 

    def test_family_tree_uris_investor(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=investor&combine_same_as_name_only=0&sources=_all&min_date=-1")
        content = str(resp.content)
        assert "Faroe Petroleum" not in content
        assert "Bowery Valuation" in content
        assert re.search(r"Family tree relationships:<\/strong>\\n\s*Investments",content) is not None
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor&combine_same_as_name_only=0&sources=_all&min_date=-1">Acquisitions</a>""" in content
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cinvestor%2Cvendor&combine_same_as_name_only=0&sources=_all&min_date=-1">All</a>""" in content 

    def test_family_tree_uris_all(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor%2Cinvestor&combine_same_as_name_only=0&sources=_all&min_date=-1")
        content = str(resp.content)
        assert "Faroe Petroleum" in content
        assert "Bowery Valuation" in content
        assert re.search(r"Family tree relationships:<\/strong>\\n\s*All",content) is not None
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor&combine_same_as_name_only=0&sources=_all&min_date=-1">Acquisitions</a>""" in content
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=investor&combine_same_as_name_only=0&sources=_all&min_date=-1">Investments</a>""" in content

    def test_query_strings_in_drill_down_linkages_source_page(self):
        client = self.client
        uri = "/organization/linkages/uri/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>Cory 1st Choice Home Delivery - Linkages</h1>" in content
        assert 'drillIntoUri(uri, root_path, "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1")' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_activity_resource_page(self):
        client = self.client
        uri = "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(uri)
        assert resp.status_code == 200
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>Resource: https://1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition</h1>" in content
        assert "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1" in content
        assert "/resource/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1" in content
        assert "/resource/1145.am/db/3558745/wwwbusinessinsidercom_jb-hunt-cory-last-mile-furniture-delivery-service-2019-1?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1" in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_linkages_from_resource_page(self):
        client = self.client
        uri = "/resource/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri,follow=True) # Will be redirected
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(uri,follow=True)
        assert resp.status_code == 200
        assert resp.redirect_chain == [('/organization/linkages/uri/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1', 302)]
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>J.B. Hunt - Linkages</h1>" in content
        assert 'drillIntoUri(uri, root_path, "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1")' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_resource_from_resource_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/resource/1145.am/db/3558745/wwwbusinessinsidercom_jb-hunt-cory-last-mile-furniture-delivery-service-2019-1?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>Resource: https://1145.am/db/3558745/wwwbusinessinsidercom_jb-hunt-cory-last-mile-furniture-delivery-service-2019-1</h1>" in content
        assert "/resource/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1" in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_family_tree_source_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/family-tree/uri/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content 
        assert "<h1>Cory 1st Choice Home Delivery - Family Tree</h1>" in content
        assert 'drillIntoUri(org_uri, "/organization/family-tree/uri/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1");' in content
        assert 'drillIntoUri(activity_uri, "/resource/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1");' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_org_from_family_tree(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/family-tree/uri/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content 
        assert "<h1>J.B. Hunt - Family Tree</h1>" in content
        assert 'drillIntoUri(org_uri, "/organization/family-tree/uri/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1");' in content
        assert 'drillIntoUri(activity_uri, "/resource/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1");' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_activity_from_family_tree(self):
        client = self.client
        client.force_login(self.user)
        uri = "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content 
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere
        assert "<h1>Resource: https://1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition</h1>" in content
        assert "/resource/1145.am/db/geonames_location/6252001?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1" in content

    def test_query_strings_in_drill_down_timeline_source_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/timeline/uri/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content 
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere
        assert "<h1>J.B. Hunt - Timeline</h1>" in content
        assert 'drillIntoUri(properties.item, "/resource/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1");' in content
        assert 'drillIntoUri(item_vals.uri, "/organization/timeline/uri/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1");' in content

    def test_query_strings_in_drill_down_family_tree_source_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/family-tree/uri/1145.am/db/2543227/Celgene?source=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert 'drillIntoUri(org_uri, "/organization/family-tree/uri/", "source=_all&min_date=-1");' in content
        assert 'drillIntoUri(activity_uri, "/resource/", "source=_all&min_date=-1");' in content
        assert len(re.findall("source=_all&min_date=-1",content)) == 16

    def test_query_strings_in_drill_down_resource_from_timeline_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat all organizations with the same name as the same organization? Off" in content 
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere
        assert "<h1>Resource: https://1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition</h1>" in content
        assert "/resource/1145.am/db/geonames_location/6252001?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&min_date=-1" in content

    def test_org_graph_filters_by_document_source_organization_defaults(self):
        client = self.client
        # Get with core sources by default
        uri_all = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?min_date=-1"
        resp_all = client.get(uri_all)
        content_all = str(resp_all.content)
        assert "PR Newswire" in content_all
        assert "CityAM" not in content_all
        assert "Business Insider" in content_all

    def test_org_graph_filters_by_document_source_organization_all(self):
        client = self.client
        # Get with all sources - if _all is in the list then all will be chosen
        uri_all = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?sources=foo,_all&min_date=-1"
        resp_all = client.get(uri_all)
        content_all = str(resp_all.content)
        assert "PR Newswire" in content_all
        assert "CityAM" in content_all
        assert "Business Insider" in content_all

    def test_org_graph_filters_by_document_source_organization_named_sources(self):
        client = self.client
        # And now with 2 specified
        uri_filtered = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?sources=CityAM,PR%20Newswire&min_date=-1"
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "PR Newswire" in content_filtered
        assert "CityAM" in content_filtered
        assert "Business Insider" not in content_filtered

    def test_org_graph_filters_by_document_source_organization_core_plus(self):
        client = self.client
        # And now with core plus an addition
        uri_filtered = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?sources=cityam,_core&min_date=-1"
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "PR Newswire" in content_filtered
        assert "CityAM" in content_filtered
        assert "Business Insider" in content_filtered

    def test_family_tree_filters_by_document_source_defaults(self):
        client = self.client
        uri_filtered = "/organization/family-tree/uri/1145.am/db/3029576/Eli_Lilly?min_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to all" in content_filtered
        assert "Switch to core" not in content_filtered
        assert "PR Newswire" in content_filtered
        assert "PR Newswire - FOO TEST" not in content_filtered
        assert "Loxo Oncology Two" not in content_filtered

    def test_family_tree_filters_by_document_source_all(self):
        client = self.client
        uri_filtered = "/organization/family-tree/uri/1145.am/db/3029576/Eli_Lilly?sources=_all&min_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to all" not in content_filtered
        assert "Switch to core" in content_filtered
        assert "PR Newswire" in content_filtered
        assert "PR Newswire - FOO TEST" in content_filtered
        assert "Loxo Oncology Two" in content_filtered

    def test_timeline_filters_by_document_source_defaults(self):
        client = self.client
        uri_filtered = "/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?min_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to all" in content_filtered
        assert "Switch to core" not in content_filtered
        assert "PR Newswire" in content_filtered
        assert "PR Newswire - FOO TEST" not in content_filtered
        assert "Loxo Oncology Two" not in content_filtered

    def test_timeline_filters_by_document_source_all(self):
        client = self.client
        uri_filtered = "/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?sources=_all&min_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to all" not in content_filtered
        assert "Switch to core" in content_filtered
        assert "PR Newswire" in content_filtered
        assert "PR Newswire - FOO TEST" in content_filtered
        assert "Loxo Oncology Two" in content_filtered

    def test_doc_date_range_linkages_old(self):
        client = self.client
        uri = "/organization/linkages/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&min_date=2020-08-26"
        resp = client.get(uri)
        content = str(resp.content)
        assert "MRI Software LLC" in content
        assert "Rental History Reports and Trusted Employees" in content
    
    def test_doc_date_range_linkages_recent(self):
        client = self.client
        uri2 = "/organization/linkages/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&min_date=2024-08-26"
        resp2 = client.get(uri2)
        content2 = str(resp2.content)
        assert "MRI Software LLC" in content2
        assert "Rental History Reports and Trusted Employees" not in content2

    def test_doc_date_range_family_tree_old(self):
        client = self.client
        uri = "/organization/family-tree/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&min_date=2020-08-26"
        client.force_login(self.user)
        resp = client.get(uri)
        content = str(resp.content)
        assert "MRI Software LLC" in content
        assert "Rental History Reports and Trusted Employees" in content
    
    def test_doc_date_range_family_tree_recent(self):
        client = self.client
        uri2 = "/organization/family-tree/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&min_date=2024-08-26"
        client.force_login(self.user)
        resp2 = client.get(uri2)
        content2 = str(resp2.content)
        assert "MRI Software LLC" in content2
        assert "Rental History Reports and Trusted Employees" not in content2

    def test_doc_date_range_timeline_old(self):
        client = self.client
        uri = "/organization/timeline/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&min_date=2020-08-26"
        client.force_login(self.user)
        resp = client.get(uri)
        content = str(resp.content)
        assert "MRI Software LLC" in content
        assert "Rental History Reports and Trusted Employees" in content
    
    def test_doc_date_range_timeline_recent(self):
        client = self.client
        uri2 = "/organization/timeline/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&min_date=2024-08-26"
        client.force_login(self.user)
        resp2 = client.get(uri2)
        content2 = str(resp2.content)
        assert "MRI Software LLC" in content2
        assert "Rental History Reports and Trusted Employees" not in content2

    def test_create_min_date_pretty_print_data_does_not_include_unecessary_dates(self):
        test_date_str = "2021-01-01"
        test_today = date(2024,1,1)
        res = create_min_date_pretty_print_data(test_date_str,test_today)
        assert res == {'min_date': date(2021, 1, 1), 'one_year_ago': date(2023, 1, 1), 
                    'one_year_ago_fmt': '2023-01-01', 'three_years_ago': None, 
                    'three_years_ago_fmt': None, 'five_years_ago': date(2020, 1, 2), 
                    'five_years_ago_fmt': '2020-01-02', 'all_time_flag': False}
    
    def test_create_min_date_pretty_print_data_with_all_time(self):
        test_date_str = "-1"
        test_today = date(2024,1,1)
        res = create_min_date_pretty_print_data(test_date_str,test_today)
        assert res == {'min_date': BEGINNING_OF_TIME, 'one_year_ago': date(2023, 1, 1), 
                       'one_year_ago_fmt': '2023-01-01', 'three_years_ago': date(2021, 1, 1), 
                       'three_years_ago_fmt': '2021-01-01', 'five_years_ago': date(2020, 1, 2), 
                       'five_years_ago_fmt': '2020-01-02', 'all_time_flag': True}

    def test_partnership_graph_data(self):
        o = Resource.nodes.get_or_none(uri='https://1145.am/db/11594/Biomax_Informatics_Ag')
        s = OrganizationGraphSerializer(o, context={"combine_same_as_name_only":True,
                                                    "source_str":"_all",
                                                    "min_date_str":"2010-01-01"})
        data = s.data
        assert len(data["node_data"]) == 7
        assert len(data["edge_data"]) == 9

    def test_product_activity_resource_serializer(self):
        n = Resource.nodes.get_or_none(uri="https://1145.am/db/10282/Launched-Version_20_Of_The_Talla_Intelligent_Knowledge_Base")
        s = ResourceSerializer(n)
        d = s.data
        assert d['resource']['product_name'] == ['version 2.0 of the Talla Intelligent Knowledge Base']
        assert len(d['relationships']) == 3

    def test_product_resource_serializer(self):
        n = Resource.nodes.get_or_none(uri="https://1145.am/db/10282/Version_20_Of_The_Talla_Intelligent_Knowledge_Base-Product")
        s = ResourceSerializer(n)
        d = s.data
        assert d['resource']['use_case'] == ['customer facing teams move deals through the pipeline faster, decrease churn, and improve customer conversations']
        assert len(d['relationships']) == 2

    def test_org_with_product_page(self):
        n = Resource.nodes.get_or_none(uri="https://1145.am/db/10282/Talla")
        g = OrganizationGraphSerializer(n, context ={"combine_same_as_name_only":True,
                                                    "source_str":"_all",
                                                    "min_date_str":"2010-01-01"})
        data = g.data
        assert len(data["node_data"]) == 4
        assert len(data["edge_data"]) == 5

    def test_product_activity_all_actors(self):
        a = Resource.nodes.get_or_none(uri='https://1145.am/db/10282/Launched-Version_20_Of_The_Talla_Intelligent_Knowledge_Base')
        acts = a.all_actors
        products = [x.uri for x in acts['product']]
        orgs = [x.uri for x in acts['organization']]
        assert products == ['https://1145.am/db/10282/Version_20_Of_The_Talla_Intelligent_Knowledge_Base-Product']
        assert orgs == ['https://1145.am/db/10282/Talla']

    def test_industry_geo_finder_prep_table(self):
        headers, ind_cluster_rows, text_row  = combined_industry_geo_results("hospital", include_search_by_industry_text=True) 
        assert headers == [OrderedDict([('Americas', {'colspan': 9, 'classes': 'col-US col-US-CA col-US-DC col-US-FL col-US-MA col-US-NY col-US-PA col-US-TX col-US-WA'}), 
                                        ('Asia', {'colspan': 1, 'classes': 'col-SA'})]), 
                            OrderedDict([('Northern America', {'colspan': 9, 'classes': 'col-US col-US-CA col-US-DC col-US-FL col-US-MA col-US-NY col-US-PA col-US-TX col-US-WA'}), 
                                         ('Western Asia', {'colspan': 1, 'classes': 'col-SA'})]), 
                            OrderedDict([('US', {'colspan': 9, 'classes': 'col-US col-US-CA col-US-DC col-US-FL col-US-MA col-US-NY col-US-PA col-US-TX col-US-WA'}), 
                                         ('SA', {'colspan': 1, 'classes': 'col-SA'})]), 
                            OrderedDict([('US (all)', {'colspan': 1, 'classes': 'col-US'}), 
                                         ('Northeast', {'colspan': 3, 'classes': 'col-US-MA col-US-NY col-US-PA'}), 
                                         ('South', {'colspan': 3, 'classes': 'col-US-DC col-US-FL col-US-TX'}), 
                                         ('West', {'colspan': 2, 'classes': 'col-US-CA col-US-WA'}), 
                                         ('REPEATED SA', {'colspan': 1, 'classes': 'col-SA'})]), 
                            OrderedDict([('REPEATED US (all)', {'colspan': 1, 'classes': 'col-US'}), 
                                         ('Mid Atlantic', {'colspan': 2, 'classes': 'col-US-NY col-US-PA'}), 
                                         ('New England', {'colspan': 1, 'classes': 'col-US-MA'}), 
                                         ('South Atlantic', {'colspan': 2, 'classes': 'col-US-DC col-US-FL'}), 
                                         ('West South Central', {'colspan': 1, 'classes': 'col-US-TX'}), 
                                         ('Pacific', {'colspan': 2, 'classes': 'col-US-CA col-US-WA'}), 
                                         ('REPEATED SA', {'colspan': 1, 'classes': 'col-SA'})]), 
                            OrderedDict([('REPEATED US (all)', {'colspan': 1, 'classes': 'col-US header_final'}), 
                                         ('US-NY', {'colspan': 1, 'classes': 'col-US-NY header_final'}), 
                                         ('US-PA', {'colspan': 1, 'classes': 'col-US-PA header_final'}), 
                                         ('US-MA', {'colspan': 1, 'classes': 'col-US-MA header_final'}), 
                                         ('US-DC', {'colspan': 1, 'classes': 'col-US-DC header_final'}), 
                                         ('US-FL', {'colspan': 1, 'classes': 'col-US-FL header_final'}), 
                                         ('US-TX', {'colspan': 1, 'classes': 'col-US-TX header_final'}), 
                                         ('US-CA', {'colspan': 1, 'classes': 'col-US-CA header_final'}), 
                                         ('US-WA', {'colspan': 1, 'classes': 'col-US-WA header_final'}), 
                                         ('REPEATED SA', {'colspan': 1, 'classes': 'col-SA header_final'})])]

        assert ind_cluster_rows[:2] == [{'uri': 'https://1145.am/db/industry/17_hospital_hospitals_hospitalist_healthcare', 
                                         'name': 'Hospital Management Service', 'industry_id': 17, 
                                         'vals': [{'value': 11, 'region_code': 'US'}, {'value': 1, 'region_code': 'US-NY'}, 
                                                  {'value': 2, 'region_code': 'US-PA'}, {'value': 0, 'region_code': 'US-MA'}, 
                                                  {'value': 0, 'region_code': 'US-DC'}, {'value': 1, 'region_code': 'US-FL'}, 
                                                  {'value': 1, 'region_code': 'US-TX'}, {'value': 3, 'region_code': 'US-CA'}, 
                                                  {'value': 1, 'region_code': 'US-WA'}, {'value': 1, 'region_code': 'SA'}]}, 
                                         {'uri': 'https://1145.am/db/industry/487_healthcare_investor_investments_investment', 
                                          'name': 'Healthcare-Dedicated Investment Firm', 'industry_id': 487, 
                                          'vals': [{'value': 1, 'region_code': 'US'}, {'value': 0, 'region_code': 'US-NY'}, 
                                                   {'value': 0, 'region_code': 'US-PA'}, {'value': 0, 'region_code': 'US-MA'}, 
                                                   {'value': 0, 'region_code': 'US-DC'}, {'value': 0, 'region_code': 'US-FL'}, 
                                                   {'value': 0, 'region_code': 'US-TX'}, {'value': 1, 'region_code': 'US-CA'}, 
                                                   {'value': 0, 'region_code': 'US-WA'}, {'value': 0, 'region_code': 'SA'}]}]
        
        assert text_row == {'uri': '', 'name': 'hospital', 
                            'vals': [{'value': 15, 'region_code': 'US'}, {'value': 2, 'region_code': 'US-NY'}, 
                                     {'value': 2, 'region_code': 'US-PA'}, {'value': 1, 'region_code': 'US-MA'}, 
                                     {'value': 1, 'region_code': 'US-DC'}, {'value': 1, 'region_code': 'US-FL'}, 
                                     {'value': 1, 'region_code': 'US-TX'}, {'value': 4, 'region_code': 'US-CA'}, 
                                     {'value': 1, 'region_code': 'US-WA'}, {'value': 1, 'region_code': 'SA'}]}

    def test_remove_not_needed_admin1s_from_individual_cells(self):
        all_industry_ids = [109, 554, 280, 223, 55, 182, 473]
        indiv_cells = [('109', 'US-NY'),('554', 'US-NY'), ('554', 'US-TX'), ('280', 'US-NY'), ('223', 'US-NY'), 
                       ('55', 'US'), ('55', 'US-TX'), ('55', 'US-CA'), ('55', 'DK'), ('182', 'US-NY'),
                       ('search_str', 'US-NY'), ('search_str', 'US-CA')]
        indiv_cells = remove_not_needed_admin1s_from_individual_cells(all_industry_ids,indiv_cells)
        assert set(indiv_cells) == set([('109', 'US-NY'), ('554', 'US-NY'), ('554', 'US-TX'), ('280', 'US-NY'), 
                                        ('223', 'US-NY'), ('55', 'US'), ('55', 'DK'), ('182', 'US-NY'),
                                        ('search_str', 'US-NY'), ('search_str', 'US-CA')])

    def test_industry_geo_finder_selection_screen(self):
        client = self.client
        resp = client.get("/industry_geo_finder?industry=software&include_search_by_industry_text=1")
        assert resp.status_code == 200
        content = str(resp.content)
        table_headers = re.findall(r"\<th.+?\>",content)
        assert table_headers == ['<th rowspan="6">',
                                    '<th colspan="13" class="isheader hascontent col-CA col-CA-08 col-US col-US-CA col-US-CT col-US-FL col-US-IL col-US-MN col-US-MS col-US-NY col-US-TN col-US-TX col-US-UT">',
                                    '<th colspan="2" class="isheader hascontent col-JP col-SG">',
                                    '<th colspan="2" class="isheader hascontent col-DE col-DK">',
                                    '<th colspan="13" class="isheader hascontent col-CA col-CA-08 col-US col-US-CA col-US-CT col-US-FL col-US-IL col-US-MN col-US-MS col-US-NY col-US-TN col-US-TX col-US-UT">',
                                    '<th colspan="1" class="isheader hascontent col-JP">',
                                    '<th colspan="1" class="isheader hascontent col-SG">',
                                    '<th colspan="1" class="isheader hascontent col-DK">',
                                    '<th colspan="1" class="isheader hascontent col-DE">',
                                    '<th colspan="2" class="isheader hascontent col-CA col-CA-08">',
                                    '<th colspan="11" class="isheader hascontent col-US col-US-CA col-US-CT col-US-FL col-US-IL col-US-MN col-US-MS col-US-NY col-US-TN col-US-TX col-US-UT">',
                                    '<th colspan="1" class="isheader hascontent col-JP">',
                                    '<th colspan="1" class="isheader hascontent col-SG">',
                                    '<th colspan="1" class="isheader hascontent col-DK">',
                                    '<th colspan="1" class="isheader hascontent col-DE">',
                                    '<th colspan="1" class="isheader hascontent col-CA">',
                                    '<th colspan="1" class="isheader hascontent col-CA-08">',
                                    '<th colspan="1" class="isheader hascontent col-US">',
                                    '<th colspan="2" class="isheader hascontent col-US-IL col-US-MN">',
                                    '<th colspan="2" class="isheader hascontent col-US-CT col-US-NY">',
                                    '<th colspan="4" class="isheader hascontent col-US-FL col-US-MS col-US-TN col-US-TX">',
                                    '<th colspan="2" class="isheader hascontent col-US-CA col-US-UT">',
                                    '<th colspan="1" class="isheader nocontent col-JP">',
                                    '<th colspan="1" class="isheader nocontent col-SG">',
                                    '<th colspan="1" class="isheader nocontent col-DK">',
                                    '<th colspan="1" class="isheader nocontent col-DE">',
                                    '<th colspan="1" class="isheader nocontent col-CA">',
                                    '<th colspan="1" class="isheader nocontent col-CA-08">',
                                    '<th colspan="1" class="isheader nocontent col-US">',
                                    '<th colspan="1" class="isheader hascontent col-US-IL">',
                                    '<th colspan="1" class="isheader hascontent col-US-MN">',
                                    '<th colspan="1" class="isheader hascontent col-US-NY">',
                                    '<th colspan="1" class="isheader hascontent col-US-CT">',
                                    '<th colspan="2" class="isheader hascontent col-US-MS col-US-TN">',
                                    '<th colspan="1" class="isheader hascontent col-US-FL">',
                                    '<th colspan="1" class="isheader hascontent col-US-TX">',
                                    '<th colspan="1" class="isheader hascontent col-US-UT">',
                                    '<th colspan="1" class="isheader hascontent col-US-CA">',
                                    '<th colspan="1" class="isheader nocontent col-JP">',
                                    '<th colspan="1" class="isheader nocontent col-SG">',
                                    '<th colspan="1" class="isheader nocontent col-DK">',
                                    '<th colspan="1" class="isheader nocontent col-DE">',
                                    '<th colspan="1" class="isheader nocontent col-CA header_final">',
                                    '<th colspan="1" class="isheader nocontent col-CA-08 header_final">',
                                    '<th colspan="1" class="isheader nocontent col-US header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-IL header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-MN header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-NY header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-CT header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-MS header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-TN header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-FL header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-TX header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-UT header_final">',
                                    '<th colspan="1" class="isheader hascontent col-US-CA header_final">',
                                    '<th colspan="1" class="isheader nocontent col-JP header_final">',
                                    '<th colspan="1" class="isheader nocontent col-SG header_final">',
                                    '<th colspan="1" class="isheader nocontent col-DK header_final">',
                                    '<th colspan="1" class="isheader nocontent col-DE header_final">']

    def test_industry_geo_finder_preview(self):
        '''
            Data shown on industry_geo_finder page in response to search "health". 'x' means entry that was chosen
                                            CA (all)	CA-08	US (all)    US-IL	US-MI	US-NY	US-PA	US-MA	US-RI	US-FL	US-MD	US-VA	US-AR	US-TX	US-AZ	US-CA	US-OR	US-WA   CN	JP	IL	SA	GB
            Health- And Beauty	            0x	        0x	    3x	        0x	    0x	    1x	    2x	    0x	    0x	    0x	    0x	    0x	    0x	    0x	    0x	    0x	    0x	    0x	    0x	0x	0x	0x	1x
            Senior Living And Health Care	0	        0	    1x	        0	    0	    0	    0	    0	    0	    0	    0	    0	    0	    0	    0	    1	    0	    0	    0	0	0	0	0
            Behavioral Health Services	    0	        0	    1x	        0	    0	    1x	    0	    0	    0	    0	    0	    0	    0	    0	    0	    0	    0	    0	    0	0	0	0	0
                    
        '''
        client = self.client
        payload = {'selectedIndividualCells': ['["row-219#col-US-NY"]'], 
                   'selectedRows': ['["row-0"]'], 'selectedColumns': ['["col-US"]'], 
                   'allIndustryIDs': ['[0, 75, 219]'], 'searchStr': ['health']}
        response = client.post("/industry_geo_finder_review",payload)
        assert response.status_code == 403
        client.force_login(self.user)
        response = client.post("/industry_geo_finder_review",payload)
        assert response.status_code == 200
        content = str(response.content)
        assert "Health- And Beauty in all Geos" in content
        assert "Behavioral Health Services, Health- And Beauty, Senior Living And Health Care in United States of America" in content
        assert "The Hilb Group" in content
        assert "Behavioral Health Services in United States of America - New York" in content

    def test_orgs_by_weight(self):
        uris = ["https://1145.am/db/3461395/Salvarx", "https://1145.am/db/2166549/Synamedia", "https://1145.am/db/3448439/Eli_Lilly_And_Company",
                "https://1145.am/db/3464614/Mufg_Union_Bank", "https://1145.am/db/3465879/Mmtec", "https://1145.am/db/3463583/Disc_Graphics",
                "https://1145.am/db/3454466/Arthur_J_Gallagher_Co", "https://1145.am/db/3029576/Celgene", "https://1145.am/db/3461324/Signal_Peak_Ventures"]
        sorted = orgs_by_connection_count(uris)
        assert sorted[0]['uri'] == 'https://1145.am/db/3454466/Arthur_J_Gallagher_Co'
        assert sorted[0]['name'] == 'Arthur J. Gallagher & Co.'
        assert sorted[-1]['uri'] == 'https://1145.am/db/3464614/Mufg_Union_Bank'
        assert sorted[-1]['name'] == 'MUFG Union Bank'
        assert len(sorted) == len(uris)

    def test_orgs_industry_is_only_by_industry_cluster_primary(self):
        client = self.client
        client.force_login(self.user)
        ind_secondary = "/industry_geo_orgs?geo_code=US-OH&industry_id=583&format=json" # indClusterSecondary
        ind_primary = "/industry_geo_orgs?geo_code=US-OH&industry_id=476&format=json" # indClusterPrimary
        resp_secondary = client.get(ind_secondary)
        resp_primary = client.get(ind_primary)
        secondary = json.loads(resp_secondary.content)
        primary = json.loads(resp_primary.content)
        assert len(primary['organizations']) == 1
        assert primary['organizations'][0]['uri'] == 'https://1145.am/db/3457045/Haines_Direct'
        assert len(secondary['organizations']) == 0

    def test_finds_similar_orgs(self):
        uri = 'https://1145.am/db/3473030/Janssen_Sciences_Ireland_Uc'
        org = Resource.nodes.get_or_none(uri=uri)
        similar = similar_organizations(org,limit=0.85)
        similar_by_ind_cluster = [(x.uri,set([z.uri for z in y])) for x,y in sorted(similar["industry_cluster"].items())]
        similar_by_ind_text = [x.uri for x in similar["industry_text"]]
        assert similar_by_ind_cluster == [('https://1145.am/db/industry/26_biopharmaceutical_biopharmaceuticals_biopharma_bioceutical', 
                                           {'https://1145.am/db/2364647/Mersana_Therapeutics', 'https://1145.am/db/2364624/Parexel_International_Corporation', 
                                            'https://1145.am/db/3473030/Eusa_Pharma', 'https://1145.am/db/2543227/Celgene', 'https://1145.am/db/3473030/Sylvant'})]
        assert set(similar_by_ind_text) == set(['https://1145.am/db/2154356/Alector', 'https://1145.am/db/3469136/Aphria_Inc', 'https://1145.am/db/2154354/Apollomics', 
                                                'https://1145.am/db/2543227/Bristol-Myers', 'https://1145.am/db/3029576/Bristol-Myers_Squibb', 
                                                'https://1145.am/db/3458145/Cannabics_Pharmaceuticals_Inc', 'https://1145.am/db/3469136/Cc_Pharma', 
                                                'https://1145.am/db/3444769/Control_Solutions_Inc', 'https://1145.am/db/11594/DSM', 'https://1145.am/db/3029576/Eli_Lilly', 
                                                'https://1145.am/db/3467694/Engility_Holdings_Inc', 'https://1145.am/db/3029576/Loxo_Oncology', 
                                                'https://1145.am/db/3469058/Napajen_Pharma', 'https://1145.am/db/3461286/Neubase', 
                                                'https://1145.am/db/3461286/Ohr_Pharmaceutical', 'https://1145.am/db/3445572/Professional_Medical_Insurance_Services', 
                                                'https://1145.am/db/3461395/Salvarx', 'https://1145.am/db/3467694/Science_Applications_International_Corp', 
                                                'https://1145.am/db/3029705/Shire', 'https://1145.am/db/2543228/Takeda'])

    def test_populates_activity_articles_for_marketing_activity(self):
        min_date = date.fromisoformat("2014-02-03")
        max_date = date.fromisoformat("2014-02-05")
        res = get_activities_by_industry_geo_and_date_range(61, "US", min_date,max_date, limit=100)
        assert len(res) == 1
        assert res[0]['activity_class'] == 'MarketingActivity'
        assert res[0]['activity_uri'] == 'https://1145.am/db/2946622/Turns_10_Years_Old'

    def test_shows_activity_articles_for_org(self):
        min_date = date.fromisoformat("2019-01-07")
        max_date = date.fromisoformat("2019-01-10")
        uri = "https://1145.am/db/2166549/Play_Sports_Group"
        org = Resource.nodes.get_or_none(uri=uri)
        res = get_activities_by_org_and_date_range(org, min_date, max_date)
        assert len(res) == 2
        assert res[0]['activity_uri'] == 'https://1145.am/db/2166549/Play_Sports_Group-Investment-Controlling'
        assert res[1]['activity_uri'] == 'https://1145.am/db/3457026/Play_Sports_Group-Investment-Controlling_Stake'
        assert res[0]['date_published'] > res[1]['date_published']

    def test_shows_org_and_activity_counts_by_industry_search_string(self):
        client = self.client
        resp = client.get("/industry_orgs_activities?industry=building&max_date=2024-04-02")
        content = str(resp.content)
        assert "Architecture, Engineering And Construction" in content
        assert "Retail Property And Place Making" in content
        assert '<a href="/industry_geo_finder_review?industry_id=686&industry=building&max_date=2024-04-02">1</a>' in content
        assert '<a href="/industry_activities?industry_id=696&min_date=2024-03-03&max_date=2024-04-02&industry=building&max_date=2024-04-02">1</a>' in content

    def test_shows_source_documents(self):
        url = "/organization/industry_geo_sources/uri/1145.am/db/3469058/Napajen_Pharma?industry_id=32&geo_code=US"
        client = self.client
        resp = client.get(url)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(url)
        assert resp.status_code == 200
        content = str(resp.content)
        assert len(re.findall("/CORRECTION/ - NapaJen Pharma, Inc./", content)) == 2
        assert len(re.findall("NapaJen Pharma Closes \$12.4 Million Series C Financing", content)) == 1

    def test_self_or_states_for_country(self):
        # Needs Dump Data for the relevant CN provinces to be defined
        assert geo_codes_for_region("GB") == {'GB'}

    def test_geo_codes_for_region_stops_when_it_gets_to_country(self):
        '''
        purpose of geo_codes_for_region is to convert a location to something to use in a query.
        If we've got CN as our country then it's not useful to replace that with several CN-XX because
        we'll want to look at all of CN, not just items tagged to the specific provinces.
        '''
        cn = geo_codes_for_region("CN")
        assert cn == {'CN'} # No need to go lower
        children = GEO_PARENT_CHILDREN['CN']['children']
        assert len(children) > 0
        assert all([re.match(r"^CN-\d\d",x) for x in children])

    def test_geo_codes_for_region_stops_when_it_gets_to_us(self):
        assert geo_codes_for_region("US") == {'US'} # Not going any deeper, even though has children
        children = GEO_PARENT_CHILDREN['US']['children']
        assert children == {'South', 'Midwest', 'Northeast', 'West'}

    
    def test_shows_tracked_organizations(self):
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
        assert "1145.am/db/2543227/Celgene" in content
        # assert "<b>All Industries</b> in <b>Australia</b>" not in content
        # assert "<b>Foo bar industry</b> in <b>All Locations</b>" not in content


    def test_shows_tracked_industry_geos(self):
        client = self.client
        client.force_login(self.user3)
        resp = client.get('/tracked_org_ind_geo')
        content = str(resp.content)
        assert "1145.am/db/2543227/Celgene" not in content
        # assert "<b>All Industries</b> in <b>Australia</b>" in content
        # assert "<b>Foo bar industry</b> in <b>All Locations</b>" in content

    def test_shows_recent_tracked_activities(self):
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

    def test_only_populates_activity_stats_if_cache_available(self):
        ''' 
            See trackeditems.tests.ActivityListTest.does_not_show_site_stats_if_no_cache
            for version without cache
        '''
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

    def api_finds_by_multiple_country_and_industry(self):
        client = self.client
        path = "/api/v1/activities/?industry_id=12&location_id=CA&location_id=IL&max_date=2019-01-10"
        resp = client.get(path)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path)
        assert resp.status_code == 200
        j = json.loads(resp.content)
        assert j['count'] == 5
        assert [x['activity_uri'] for x in j['results']] == ['https://1145.am/db/3557548/Canadian_Imperial_Bank_Of_Commerce-Pharmhouse-Investment', 
                            'https://1145.am/db/3463554/Ams-Acquisition', 'https://1145.am/db/3458145/Cannabics_Pharmaceuticals_Inc-Seedo_Corp-Seedo_Corp-Investment', 
                            'https://1145.am/db/3457416/Isracann_Biosciences_Inc-Investment', 'https://1145.am/db/3453527/Canntrust_Holdings_Inc-Ipo-Common_Shares']

    def api_finds_by_single_country_and_industry(self):
        client = self.client
        path = "/api/v1/activities/?industry_id=12&location_id=CA&max_date=2019-01-10"
        client.force_login(self.user)
        resp = client.get(path)
        j = json.loads(resp.content)
        assert j['count'] == 3
        assert [x['activity_uri'] for x in j['results']] == ['https://1145.am/db/3557548/Canadian_Imperial_Bank_Of_Commerce-Pharmhouse-Investment', 
                                                             'https://1145.am/db/3463554/Ams-Acquisition', 
                                                             'https://1145.am/db/3453527/Canntrust_Holdings_Inc-Ipo-Common_Shares']
