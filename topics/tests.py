from django.test import TestCase
from collections import OrderedDict
from topics.models import *
from .stats_helpers import get_stats, date_minus
from auth_extensions.anon_user_utils import create_anon_user
from .activity_helpers import (get_activities_by_country_and_date_range, activities_by_industry, 
            activities_by_region, get_activities_by_industry_geo_and_date_range,
            activity_articles_to_api_results,
            )
from .family_tree_helpers import get_parent_orgs, get_child_orgs
from topics.graph_utils import graph_centered_on
from topics.timeline_utils import get_timeline_data
import os
from integration.management.commands.import_ttl import do_import_ttl
from integration.models import DataImport
from neomodel import db
from datetime import date, datetime, timezone
import time
from django.contrib.auth import get_user_model
from topics.serializers import *
from integration.neo4j_utils import delete_all_not_needed_resources
from integration.rdf_post_processor import RDFPostProcessor
from integration.tests import make_node, clean_db
import json
import re
from .serializers import (
    only_valid_relationships, FamilyTreeSerializer, 
    create_earliest_date_pretty_print_data,
)
from topics.industry_geo.orgs_by_industry_geo import (
    build_region_hierarchy, prepare_headers,
    combined_industry_geo_results,
)
from topics.industry_geo.hierarchy_utils import filtered_hierarchy, hierarchy_widths
from topics.cache_helpers import refresh_geo_data, nuke_cache
from topics.industry_geo import orgs_by_industry_and_or_geo
from topics.views import remove_not_needed_admin1s_from_individual_cells
from dump.embeddings.embedding_utils import apply_latest_org_embeddings
import pickle

'''
    Care these tests will delete neodb data
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)

class TestUtilsWithDumpData(TestCase):

    def setUpTestData():
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() == None # Empty DB
        do_import_ttl(dirname="dump",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources()
        r = RDFPostProcessor()
        r.run_all_in_order()
        refresh_geo_data()
        apply_latest_org_embeddings()

    def setUp(self):
        ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{ts}")
        self.anon, _ = create_anon_user()

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
        expected = set(['https://1145.am/db/1736082/Berlin', 'https://1145.am/db/1736082/Brandenburg', 
                                    'https://1145.am/db/1736082/Gr_Enheide', 'https://1145.am/db/1736082/Tesla', 'https://1145.am/db/1736082/Tesla-Added-Berlin', 
                                    'https://1145.am/db/1736082/techcrunchcom_2019_12_21_tesla-nears-land-deal-for-german-gigafactory-outside-of-berlin_', 
                                    'https://1145.am/db/geonames_location/2921044', 'https://1145.am/db/geonames_location/2945356', 'https://1145.am/db/geonames_location/2950159', 
                                    'https://1145.am/db/geonames_location/553898', 'https://1145.am/db/industry/302_automakers_carmakers_automaker_automaking'])
        assert clean_uris == expected , f"Got {clean_uris} - diff = {clean_uris.symmetric_difference(expected)}"
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
        path = "/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=1&sources=_all&earliest_date=-1"
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
        response = client.get("/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=1&sources=_all&earliest_date=-1")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 114
        assert "technologies" in content # from sameAsNameOnly's industry

    def test_organization_graph_view_without_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=0&sources=_all&earliest_date=-1")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 50
        assert "technologies" not in content

    def test_stats(self):
        max_date = date.fromisoformat("2024-06-02")
        counts, recents_by_geo, recents_by_source, recents_by_industry = get_stats(max_date)
        assert set(counts) == {('AboutUs', 1), ('Person', 12), ('OperationsActivity', 4), ('IncidentActivity', 1), 
                               ('RecognitionActivity', 1), ('EquityActionsActivity', 2), ('PartnershipActivity', 4), 
                               ('ProductActivity', 10), ('RegulatoryActivity', 1), ('FinancialReportingActivity', 1),
                               ('MarketingActivity', 3), ('FinancialsActivity', 2), ('Organization', 432), ('Article', 213), 
                               ('CorporateFinanceActivity', 194), 
                               ('AnalystRatingActivity', 1), ('LocationActivity', 11), ('Role', 11), ('RoleActivity', 12)}

        assert sorted(recents_by_geo) == [('AU', 'Australia', 1, 1, 1), ('CA', 'Canada', 3, 3, 3), ('CN', 'China', 2, 2, 2), 
                                          ('CZ', 'Czechia', 1, 1, 1), ('DK', 'Denmark', 1, 1, 1), ('EG', 'Egypt', 0, 0, 1), 
                                          ('ES', 'Spain', 1, 1, 1), ('GB', 'United Kingdom of Great Britain and Northern Ireland', 3, 3, 3), 
                                          ('IE', 'Ireland', 1, 1, 1), ('IL', 'Israel', 1, 1, 1), ('IT', 'Italy', 1, 1, 1), ('JP', 'Japan', 0, 0, 1), 
                                          ('KE', 'Kenya', 1, 1, 1), ('UG', 'Uganda', 1, 1, 1), ('US', 'United States of America', 16, 16, 37)]
        sample_acts = activities_by_region("AU",date_minus(max_date,7),max_date,counts_only=False)
        assert sample_acts == [['https://1145.am/db/4290457/Gyg-Ipo', 
                                'https://1145.am/db/4290457/wwwreuterscom_markets_deals_australian-fast-food-chain-guzman-y-gomez-seeks-raise-161-mln-june-ipo-2024-05-31_', 
                                datetime(2024, 5, 31, 5, 12, 37, 320000, tzinfo=timezone.utc)]]
        sample_acts = activities_by_region("GB",date_minus(max_date,7),max_date,counts_only=False)
        assert len(sample_acts) == 3
        assert ['https://1145.am/db/4290472/Associated_British_Foods-Ipo', 'https://1145.am/db/4290472/wwwmarketwatchcom_story_ab-foods-majority-shareholder-sells-10-3-mln-shares-for-gbp262-mln-067222fe', datetime(2024, 5, 31, 6, 42, tzinfo=timezone.utc)] in sample_acts
        assert ['https://1145.am/db/3474027/Aquiline_Technology_Growth-Gan-Investment-Series_B', 'https://1145.am/db/3474027/wwwprnewswirecom_news-releases_gan-integrity-raises-15-million-to-accelerate-global-compliance-solution-300775390html', datetime(2024, 5, 29, 13, 15, tzinfo=timezone.utc)] in sample_acts
        assert ['https://1145.am/db/3473030/Sylvant-Acquisition-Rights', 'https://1145.am/db/3473030/wwwprnewswirecom_news-releases_eusa-pharma-completes-acquisition-of-global-rights-to-sylvant-siltuximab--and-presents-company-update-at-37th-jp-morgan-healthcare-conference-300775508html', datetime(2024, 5, 29, 13, 0, tzinfo=timezone.utc)] in sample_acts
        assert sorted(recents_by_source) == [('Associated Press', 3, 3, 3), ('Business Insider', 2, 2, 2), ('Business Wire', 1, 1, 1), 
                                             ('CityAM', 1, 1, 4), ('Fierce Pharma', 0, 0, 3), ('GlobeNewswire', 2, 2, 2), 
                                             ('Hotel Management', 0, 0, 1), ('Live Design Online', 0, 0, 1), ('MarketWatch', 3, 3, 3), 
                                             ('PR Newswire', 20, 20, 33), ('Reuters', 1, 1, 1), ('TechCrunch', 0, 0, 1), 
                                             ('The Globe and Mail', 1, 1, 1), ('VentureBeat', 0, 0, 1)]
        assert recents_by_industry[:10] == [(696, 'Architectural And Design', 0, 0, 1), (154, 'Biomanufacturing Technologies', 0, 0, 3), 
                                                (26, 'Biopharmaceutical And Biotech Industry', 1, 1, 6), (36, 'C-Commerce (\\', 1, 1, 1), (12, 'Cannabis And Hemp', 1, 1, 1), 
                                                (236, 'Chemical And Technology', 0, 0, 1), (74, 'Chip Business', 1, 1, 1), (4, 'Cloud Services', 0, 0, 1), 
                                                (165, 'Development Banks', 1, 1, 1), (134, 'Electronic Manufacturing Services And Printed Circuit Board Assembly', 1, 1, 1)]
        sample_ind = IndustryCluster.nodes.get_or_none(topicId=154)
        res = activities_by_industry(sample_ind,date_minus(max_date,90),max_date,counts_only=False)
        assert len(res) == 3
        assert ['https://1145.am/db/3029576/Tesaro-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)] in res
        assert ['https://1145.am/db/3029576/Loxo_Oncology-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)] in res 
        assert ['https://1145.am/db/3029576/Celgene-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)] in res

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
        orgs = orgs_by_industry_and_or_geo(industry,selected_geo)
        assert len(orgs) == 2

    def test_search_by_industry_only(self):
        selected_geo_name = ""
        industry_name = "Biopharmaceutical And Biotech Industry"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None
        orgs = orgs_by_industry_and_or_geo(industry,selected_geo)
        assert set(orgs) == {'https://1145.am/db/3473030/Eusa_Pharma', 'https://1145.am/db/2364624/Parexel_International_Corporation', 
                             'https://1145.am/db/2364647/Mersana_Therapeutics', 'https://1145.am/db/3473030/Janssen_Sciences_Ireland_Uc', 
                             'https://1145.am/db/2543227/Celgene', 'https://1145.am/db/3473030/Sylvant'}

    def test_search_by_geo_only(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = ""
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is None
        orgs = orgs_by_industry_and_or_geo(industry,selected_geo)
        assert len(orgs) == 8
        assert set(orgs) == set(['https://1145.am/db/3452608/Avon_Products_Inc', 
                             'https://1145.am/db/3465815/Alliance_Automotive_Group', 
                             'https://1145.am/db/1787315/Scape', 'https://1145.am/db/3473030/Eusa_Pharma', 
                             'https://1145.am/db/2364647/Mersana_Therapeutics', 'https://1145.am/db/2946625/Cuadrilla_Resources', 
                             'https://1145.am/db/3029681/Halebury', 'https://1145.am/db/3465883/Pistonheads'])

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
        path = "/organization/family-tree/uri/1145.am/db/3451381/Responsability_Investments_Ag?combine_same_as_name_only=0&rels=buyer,investor,vendor&earliest_date=-1"
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
        resp = client.get("/organization/family-tree/uri/1145.am/db/3451381/Responsability_Investments_Ag?combine_same_as_name_only=1&rels=buyer,investor,vendor&earliest_date=-1")
        content = str(resp.content)
        assert "REDAVIA" in content
        assert "REDOVIA" in content

    def test_company_search_with_combine_same_as_name_only(self):
        client = self.client
        resp = client.get("/?name=eli&combine_same_as_name_only=1&earliest_date=-1")
        content = str(resp.content)
        assert "Eli Lilly" in content
        assert "https://1145.am/db/3029576/Eli_Lilly" in content
        assert "Eli Lilly and Company" not in content
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" not in content

    def test_company_search_without_combine_same_as_name_only(self):
        client = self.client
        resp = client.get("/?name=eli&combine_same_as_name_only=0&earliest_date=-1")
        content = str(resp.content)
        assert "Eli Lilly" in content
        assert "https://1145.am/db/3029576/Eli_Lilly" in content
        assert "Eli Lilly and Company" in content
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" in content

    def test_graph_combines_same_as_name_only_off_vs_on_based_on_target_node(self):
        client = self.client
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0&earliest_date=-1")
        content0 = str(resp.content)
        activities0 = len(re.findall("Acquisition",content0))
        eli_lillies0 = len(re.findall("Eli Lilly",content0))
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=1&earliest_date=-1")
        content1 = str(resp.content)
        activities1 = len(re.findall("Acquisition",content1))
        eli_lillies1 = len(re.findall("Eli Lilly",content1))
        assert activities0 == activities1
        assert eli_lillies0 > eli_lillies1 # when combined same as name only have fewer orgs

    def test_graph_combines_same_as_name_only_off_vs_on_based_on_central_node(self):
        client = self.client
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=0&earliest_date=-1")
        content0 = str(resp.content)
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=1&earliest_date=-1")
        content1 = str(resp.content)
        assert "https://1145.am/db/3464715/Loxo_Oncology-Acquisition" in content0
        assert "https://1145.am/db/3464715/Loxo_Oncology-Acquisition" in content1
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" not in content0
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" in content1

    def test_timeline_combines_same_as_name_only_on_off(self):
        client = self.client
        path0 = "/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=0&earliest_date=-1"
        resp = client.get(path0)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path0)
        assert resp.status_code == 200
        content0 = str(resp.content)
        resp = client.get("/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=1&earliest_date=-1")
        content1 = str(resp.content)
        assert "https://1145.am/db/3549221/Loxo_Oncology-Acquisition" in content0
        assert "https://1145.am/db/3549221/Loxo_Oncology-Acquisition" in content1
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" not in content0
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" in content1

    def test_family_tree_same_as_name_only_on_off_parents(self):
        client = self.client
        path0 = "/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0&sources=_all&earliest_date=-1"
        resp = client.get(path0)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(path0)
        assert resp.status_code == 200
        content0 = str(resp.content)
        resp = client.get("/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=1&sources=_all&earliest_date=-1")
        content1 = str(resp.content)

        assert "https://1145.am/db/3029576/Eli_Lilly" in content0
        assert "https://1145.am/db/3029576/Eli_Lilly" in content1
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" in content0
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" not in content1
        assert "Buyer (CityAM Mar 2024)" in content0
        assert "Buyer (CityAM Mar 2024)" in content1
        assert "Buyer (PR Newswire Jan 2019)" in content0
        assert "Buyer (PR Newswire Jan 2019)" not in content1

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
        resp = client.get("/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor&combine_same_as_name_only=0&sources=_all&earliest_date=-1")
        content = str(resp.content)
        assert "Faroe Petroleum" in content
        assert "Bowery Valuation" not in content
        assert re.search(r"Family tree relationships:<\/strong>\\n\s*Acquisitions",content) is not None
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=investor&combine_same_as_name_only=0&sources=_all&earliest_date=-1">Investments</a>""" in content
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cinvestor%2Cvendor&combine_same_as_name_only=0&sources=_all&earliest_date=-1">All</a>""" in content 

    def test_family_tree_uris_investor(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=investor&combine_same_as_name_only=0&sources=_all&earliest_date=-1")
        content = str(resp.content)
        assert "Faroe Petroleum" not in content
        assert "Bowery Valuation" in content
        assert re.search(r"Family tree relationships:<\/strong>\\n\s*Investments",content) is not None
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor&combine_same_as_name_only=0&sources=_all&earliest_date=-1">Acquisitions</a>""" in content
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cinvestor%2Cvendor&combine_same_as_name_only=0&sources=_all&earliest_date=-1">All</a>""" in content 

    def test_family_tree_uris_all(self):
        client = self.client
        client.force_login(self.user)
        resp = client.get("/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor%2Cinvestor&combine_same_as_name_only=0&sources=_all&earliest_date=-1")
        content = str(resp.content)
        assert "Faroe Petroleum" in content
        assert "Bowery Valuation" in content
        assert re.search(r"Family tree relationships:<\/strong>\\n\s*All",content) is not None
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=buyer%2Cvendor&combine_same_as_name_only=0&sources=_all&earliest_date=-1">Acquisitions</a>""" in content
        assert """<a href="/organization/family-tree/uri/1145.am/db/1786805/Camber_Creek?rels=investor&combine_same_as_name_only=0&sources=_all&earliest_date=-1">Investments</a>""" in content

    def test_query_strings_in_drill_down_linkages_source_page(self):
        client = self.client
        uri = "/organization/linkages/uri/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>Cory 1st Choice Home Delivery - Linkages</h1>" in content
        assert 'drillIntoUri(uri, root_path, "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1")' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_activity_resource_page(self):
        client = self.client
        uri = "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(uri)
        assert resp.status_code == 200
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>Resource: https://1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition</h1>" in content
        assert "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1" in content
        assert "/resource/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1" in content
        assert "/resource/1145.am/db/3558745/wwwbusinessinsidercom_jb-hunt-cory-last-mile-furniture-delivery-service-2019-1?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1" in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_linkages_from_resource_page(self):
        client = self.client
        uri = "/resource/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri,follow=True) # Will be redirected
        assert resp.status_code == 403
        client.force_login(self.user)
        resp = client.get(uri,follow=True)
        assert resp.status_code == 200
        assert resp.redirect_chain == [('/organization/linkages/uri/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1', 302)]
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>J.B. Hunt - Linkages</h1>" in content
        assert 'drillIntoUri(uri, root_path, "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1")' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_resource_from_resource_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/resource/1145.am/db/3558745/wwwbusinessinsidercom_jb-hunt-cory-last-mile-furniture-delivery-service-2019-1?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content # confirm that combine_same_as_name_only=0 is being applied 
        assert "<h1>Resource: https://1145.am/db/3558745/wwwbusinessinsidercom_jb-hunt-cory-last-mile-furniture-delivery-service-2019-1</h1>" in content
        assert "/resource/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1" in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_family_tree_source_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/family-tree/uri/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content 
        assert "<h1>Cory 1st Choice Home Delivery - Family Tree</h1>" in content
        assert 'drillIntoUri(org_uri, "/organization/family-tree/uri/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1");' in content
        assert 'drillIntoUri(activity_uri, "/resource/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1");' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_org_from_family_tree(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/family-tree/uri/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content 
        assert "<h1>J.B. Hunt - Family Tree</h1>" in content
        assert 'drillIntoUri(org_uri, "/organization/family-tree/uri/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1");' in content
        assert 'drillIntoUri(activity_uri, "/resource/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1");' in content
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere

    def test_query_strings_in_drill_down_activity_from_family_tree(self):
        client = self.client
        client.force_login(self.user)
        uri = "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content 
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere
        assert "<h1>Resource: https://1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition</h1>" in content
        assert "/resource/1145.am/db/geonames_location/6252001?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1" in content

    def test_query_strings_in_drill_down_timeline_source_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/timeline/uri/1145.am/db/3558745/Jb_Hunt?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content 
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere
        assert "<h1>J.B. Hunt - Timeline</h1>" in content
        assert 'drillIntoUri(properties.item, "/resource/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1");' in content
        assert 'drillIntoUri(item_vals.uri, "/organization/timeline/uri/", "abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1");' in content

    def test_query_strings_in_drill_down_family_tree_source_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/organization/family-tree/uri/1145.am/db/2543227/Celgene?source=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert 'drillIntoUri(org_uri, "/organization/family-tree/uri/", "source=_all&earliest_date=-1");' in content
        assert 'drillIntoUri(activity_uri, "/resource/", "source=_all&earliest_date=-1");' in content
        assert len(re.findall("source=_all&earliest_date=-1",content)) == 15

    def test_query_strings_in_drill_down_resource_from_timeline_page(self):
        client = self.client
        client.force_login(self.user)
        uri = "/resource/1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1"
        resp = client.get(uri)
        content = str(resp.content)
        assert "Treat sameAsNameOnly relationship as same? No" in content 
        assert "&amp;combine" not in content # Ensure & in query string is not escaped anywhere
        assert "<h1>Resource: https://1145.am/db/3558745/Cory_1st_Choice_Home_Delivery-Acquisition</h1>" in content
        assert "/resource/1145.am/db/geonames_location/6252001?abc=def&ged=123&combine_same_as_name_only=0&rels=buyer%2Cvendor&sources=_all&earliest_date=-1" in content

    def test_org_graph_filters_by_documennt_source_organization_defaults(self):
        client = self.client
        # Get with core sources by default
        uri_all = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?earliest_date=-1"
        resp_all = client.get(uri_all)
        content_all = str(resp_all.content)
        assert "PR Newswire" in content_all
        assert "CityAM" not in content_all
        assert "Business Insider" in content_all

    def test_org_graph_filters_by_document_source_organization_all(self):
        client = self.client
        # Get with all sources - if _all is in the list then all will be chosen
        uri_all = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?sources=foo,_all&earliest_date=-1"
        resp_all = client.get(uri_all)
        content_all = str(resp_all.content)
        assert "PR Newswire" in content_all
        assert "CityAM" in content_all
        assert "Business Insider" in content_all

    def test_org_graph_filters_by_document_source_organization_named_sources(self):
        client = self.client
        # And now with 2 specified
        uri_filtered = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?sources=CityAM,PR%20Newswire&earliest_date=-1"
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "PR Newswire" in content_filtered
        assert "CityAM" in content_filtered
        assert "Business Insider" not in content_filtered

    def test_org_graph_filters_by_document_source_organization_core_plus(self):
        client = self.client
        # And now with core plus an addition
        uri_filtered = "/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?sources=cityam,_core&earliest_date=-1"
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "PR Newswire" in content_filtered
        assert "CityAM" in content_filtered
        assert "Business Insider" in content_filtered

    def test_family_tree_filters_by_document_source_defaults(self):
        client = self.client
        uri_filtered = "/organization/family-tree/uri/1145.am/db/3029576/Eli_Lilly?earliest_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to all" in content_filtered
        content_filtered = re.sub(r"Document sources:.+Switch to all","",content_filtered)
        assert "PR Newswire" in content_filtered # CityAM story is newer but not included
        assert "CityAM" not in content_filtered # Not available in core

    def test_family_tree_filters_by_document_source_all(self):
        client = self.client
        uri_filtered = "/organization/family-tree/uri/1145.am/db/3029576/Eli_Lilly?sources=_all&earliest_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to core" in content_filtered
        content_filtered = re.sub(r"Document sources:.+Switch to core","",content_filtered)
        assert "PR Newswire" not in content_filtered # Was shown in the Document sources list but not in body of the graph
        assert "CityAM" in content_filtered # Newest dated version

    def test_timeline_filters_by_document_source_defaults(self):
        client = self.client
        uri_filtered = "/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?earliest_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to all" in content_filtered
        content_filtered = re.sub(r"Document sources:.+Switch to all","",content_filtered)
        assert "PR Newswire" in content_filtered 
        assert "CityAM" not in content_filtered # Not available in core

    def test_timeline_filters_by_document_source_all(self):
        client = self.client
        uri_filtered = "/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?sources=_all&earliest_date=-1"
        client.force_login(self.user)
        resp_filtered = client.get(uri_filtered)
        content_filtered = str(resp_filtered.content)
        assert "Switch to core" in content_filtered
        content_filtered = re.sub(r"Document sources:.+Switch to core","",content_filtered)
        assert "PR Newswire" in content_filtered 
        assert "CityAM" in content_filtered

    def test_doc_date_range_linkages_old(self):
        client = self.client
        uri = "/organization/linkages/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&earliest_date=2020-08-26"
        resp = client.get(uri)
        content = str(resp.content)
        assert "MRI Software LLC" in content
        assert "Rental History Reports and Trusted Employees" in content
    
    def test_doc_date_range_linkages_recent(self):
        client = self.client
        uri2 = "/organization/linkages/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&earliest_date=2024-08-26"
        resp2 = client.get(uri2)
        content2 = str(resp2.content)
        assert "MRI Software LLC" in content2
        assert "Rental History Reports and Trusted Employees" not in content2

    def test_doc_date_range_family_tree_old(self):
        client = self.client
        uri = "/organization/family-tree/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&earliest_date=2020-08-26"
        client.force_login(self.user)
        resp = client.get(uri)
        content = str(resp.content)
        assert "MRI Software LLC" in content
        assert "Rental History Reports and Trusted Employees" in content
    
    def test_doc_date_range_family_tree_recent(self):
        client = self.client
        uri2 = "/organization/family-tree/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&earliest_date=2024-08-26"
        client.force_login(self.user)
        resp2 = client.get(uri2)
        content2 = str(resp2.content)
        assert "MRI Software LLC" in content2
        assert "Rental History Reports and Trusted Employees" not in content2

    def test_doc_date_range_timeline_old(self):
        client = self.client
        uri = "/organization/timeline/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&earliest_date=2020-08-26"
        client.force_login(self.user)
        resp = client.get(uri)
        content = str(resp.content)
        assert "MRI Software LLC" in content
        assert "Rental History Reports and Trusted Employees" in content
    
    def test_doc_date_range_timeline_recent(self):
        client = self.client
        uri2 = "/organization/timeline/uri/1145.am/db/3475312/Mri_Software_Llc?sources=_all&earliest_date=2024-08-26"
        client.force_login(self.user)
        resp2 = client.get(uri2)
        content2 = str(resp2.content)
        assert "MRI Software LLC" in content2
        assert "Rental History Reports and Trusted Employees" not in content2

    def test_create_earliest_date_pretty_print_data_does_not_include_unecessary_dates(self):
        test_date_str = "2021-01-01"
        test_today = date(2024,1,1)
        res = create_earliest_date_pretty_print_data(test_date_str,test_today)
        assert res == {'min_date': date(2021, 1, 1), 'one_year_ago': date(2023, 1, 1), 
                    'one_year_ago_fmt': '2023-01-01', 'three_years_ago': None, 
                    'three_years_ago_fmt': None, 'five_years_ago': date(2020, 1, 2), 
                    'five_years_ago_fmt': '2020-01-02', 'all_time_flag': False}
    
    def test_create_earliest_date_pretty_print_data_with_all_time(self):
        test_date_str = "-1"
        test_today = date(2024,1,1)
        res = create_earliest_date_pretty_print_data(test_date_str,test_today)
        assert res == {'min_date': BEGINNING_OF_TIME, 'one_year_ago': date(2023, 1, 1), 
                       'one_year_ago_fmt': '2023-01-01', 'three_years_ago': date(2021, 1, 1), 
                       'three_years_ago_fmt': '2021-01-01', 'five_years_ago': date(2020, 1, 2), 
                       'five_years_ago_fmt': '2020-01-02', 'all_time_flag': True}

    def test_partnership_graph_data(self):
        o = Resource.nodes.get_or_none(uri='https://1145.am/db/11594/Biomax_Informatics_Ag')
        s = OrganizationGraphSerializer(o, context={"combine_same_as_name_only":True,
                                                    "source_str":"_all",
                                                    "earliest_str":"2010-01-01"})
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
                                                    "earliest_str":"2010-01-01"})
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
        headers, ind_cluster_rows, text_row  = combined_industry_geo_results("hospital") 
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
        resp = client.get("/industry_geo_finder?industry=software")
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
        sorted = orgs_by_weight(uris)
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
        similar = org.similar_organizations(limit=0.85)
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


class TestRegionHierarchy(TestCase):

    def setUpTestData():
        nuke_cache()

    def test_builds_region_hierarchy(self):
        countries = ["GB","CN","CA","US","ZA","AE","SG","NA","SZ"]
        admin1s = {"US":["IL","OK","IA","NY"],"CA":["12","13"],"CN":["11","04"]}

        country_hierarchy, country_widths, admin1_hierarchy, admin1_widths = build_region_hierarchy(countries, admin1s)

        assert country_hierarchy == {'Asia': {'Western Asia': ['AE'], 
                                              'South-eastern Asia': ['SG'],
                                              'Eastern Asia': ['CN']}, 
                                     'Europe': {'Northern Europe': ['GB']}, 
                                     'Africa': {'Sub-Saharan Africa': {'Southern Africa': ['NA', 'SZ', 'ZA']}}, 
                                     'Americas': {'Northern America': ['CA', 'US']}}

        assert country_widths == {'Asia#Western Asia': 1, 'Asia#South-eastern Asia': 1, 'Asia#Eastern Asia': 1, 
                                  'Europe#Northern Europe': 1, 'Africa#Sub-Saharan Africa#Southern Africa': 3, 
                                  'Americas#Northern America': 2, 'Americas': 2, 'Africa#Sub-Saharan Africa': 3, 
                                  'Africa': 3, 'Europe': 1, 'Asia': 3}

        assert admin1_hierarchy == {'US': {'Northeast': {'Mid Atlantic': ['NY']}, 
                                           'Midwest': {'East North Central': ['IL'], 
                                                       'West North Central': ['IA']}, 
                                            'South': {'West South Central': ['OK']}}, 
                                    'CA': ['12', '13'], 'CN': ['04', '11']}

        assert admin1_widths == {'US': {'US#Northeast#Mid Atlantic': 1, 'US#Midwest#East North Central': 1, 
                                        'US#Midwest#West North Central': 1, 'US#South#West South Central': 1, 
                                        'US#South': 1, 'US': 4, 'US#Midwest': 2, 'US#Northeast': 1}, 
                                        'CA': {'CA': 2}, 'CN': {'CN': 2}}
            
        headers = prepare_headers(country_hierarchy, country_widths, admin1_hierarchy, admin1_widths, countries, admin1s)
        assert headers[0] == OrderedDict([('Africa', {'colspan': 3, 'classes': 'col-NA col-SZ col-ZA'}), 
                                          ('Americas', {'colspan': 8, 'classes': 'col-CA col-CA-12 col-CA-13 col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('Asia', {'colspan': 5, 'classes': 'col-AE col-CN col-CN-04 col-CN-11 col-SG'}), 
                                          ('Europe', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[1] == OrderedDict([('Sub-Saharan Africa', {'colspan': 3, 'classes': 'col-NA col-SZ col-ZA'}), 
                                          ('Northern America', {'colspan': 8, 'classes': 'col-CA col-CA-12 col-CA-13 col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('Eastern Asia', {'colspan': 3, 'classes': 'col-CN col-CN-04 col-CN-11'}), 
                                          ('South-eastern Asia', {'colspan': 1, 'classes': 'col-SG'}), ('Western Asia', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('Northern Europe', {'colspan': 1, 'classes': 'col-GB'})])  
        assert headers[2] == OrderedDict([('Southern Africa', {'colspan': 3, 'classes': 'col-NA col-SZ col-ZA'}), 
                                          ('REPEATED Northern America', {'colspan': 8, 'classes': 'col-CA col-CA-12 col-CA-13 col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('REPEATED Eastern Asia', {'colspan': 3, 'classes': 'col-CN col-CN-04 col-CN-11'}), 
                                          ('REPEATED South-eastern Asia', {'colspan': 1, 'classes': 'col-SG'}), 
                                          ('REPEATED Western Asia', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('REPEATED Northern Europe', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[3] == OrderedDict([('NA', {'colspan': 1, 'classes': 'col-NA'}), ('SZ', {'colspan': 1, 'classes': 'col-SZ'}), ('ZA', {'colspan': 1, 'classes': 'col-ZA'}), 
                                          ('CA', {'colspan': 3, 'classes': 'col-CA col-CA-12 col-CA-13'}), ('US', {'colspan': 5, 'classes': 'col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('CN', {'colspan': 3, 'classes': 'col-CN col-CN-04 col-CN-11'}), ('SG', {'colspan': 1, 'classes': 'col-SG'}), 
                                          ('AE', {'colspan': 1, 'classes': 'col-AE'}), ('GB', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[4] == OrderedDict([('REPEATED NA', {'colspan': 1, 'classes': 'col-NA'}), ('REPEATED SZ', {'colspan': 1, 'classes': 'col-SZ'}), 
                                          ('REPEATED ZA', {'colspan': 1, 'classes': 'col-ZA'}), ('CA (all)', {'colspan': 1, 'classes': 'col-CA'}), 
                                          ('CA-12', {'colspan': 1, 'classes': 'col-CA-12'}), ('CA-13', {'colspan': 1, 'classes': 'col-CA-13'}), 
                                          ('US (all)', {'colspan': 1, 'classes': 'col-US'}), ('Midwest', {'colspan': 2, 'classes': 'col-US-IA col-US-IL'}), 
                                          ('Northeast', {'colspan': 1, 'classes': 'col-US-NY'}), ('South', {'colspan': 1, 'classes': 'col-US-OK'}), 
                                          ('CN (all)', {'colspan': 1, 'classes': 'col-CN'}), ('CN-04', {'colspan': 1, 'classes': 'col-CN-04'}), 
                                          ('CN-11', {'colspan': 1, 'classes': 'col-CN-11'}), ('REPEATED SG', {'colspan': 1, 'classes': 'col-SG'}), 
                                          ('REPEATED AE', {'colspan': 1, 'classes': 'col-AE'}), ('REPEATED GB', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[5] == OrderedDict([('REPEATED NA', {'colspan': 1, 'classes': 'col-NA'}), ('REPEATED SZ', {'colspan': 1, 'classes': 'col-SZ'}), 
                                          ('REPEATED ZA', {'colspan': 1, 'classes': 'col-ZA'}), ('REPEATED CA (all)', {'colspan': 1, 'classes': 'col-CA'}), 
                                          ('REPEATED CA-12', {'colspan': 1, 'classes': 'col-CA-12'}), ('REPEATED CA-13', {'colspan': 1, 'classes': 'col-CA-13'}), 
                                          ('REPEATED US (all)', {'colspan': 1, 'classes': 'col-US'}), ('East North Central', {'colspan': 1, 'classes': 'col-US-IL'}), 
                                          ('West North Central', {'colspan': 1, 'classes': 'col-US-IA'}), ('Mid Atlantic', {'colspan': 1, 'classes': 'col-US-NY'}), 
                                          ('West South Central', {'colspan': 1, 'classes': 'col-US-OK'}), ('REPEATED CN (all)', {'colspan': 1, 'classes': 'col-CN'}), 
                                          ('REPEATED CN-04', {'colspan': 1, 'classes': 'col-CN-04'}), ('REPEATED CN-11', {'colspan': 1, 'classes': 'col-CN-11'}), 
                                          ('REPEATED SG', {'colspan': 1, 'classes': 'col-SG'}), ('REPEATED AE', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('REPEATED GB', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[6] == OrderedDict([('REPEATED NA', {'colspan': 1, 'classes': 'col-NA header_final'}), ('REPEATED SZ', {'colspan': 1, 'classes': 'col-SZ header_final'}), 
                                          ('REPEATED ZA', {'colspan': 1, 'classes': 'col-ZA header_final'}), ('REPEATED CA (all)', {'colspan': 1, 'classes': 'col-CA header_final'}), 
                                          ('REPEATED CA-12', {'colspan': 1, 'classes': 'col-CA-12 header_final'}), ('REPEATED CA-13', {'colspan': 1, 'classes': 'col-CA-13 header_final'}), 
                                          ('REPEATED US (all)', {'colspan': 1, 'classes': 'col-US header_final'}), ('US-IL', {'colspan': 1, 'classes': 'col-US-IL header_final'}), 
                                          ('US-IA', {'colspan': 1, 'classes': 'col-US-IA header_final'}), ('US-NY', {'colspan': 1, 'classes': 'col-US-NY header_final'}), 
                                          ('US-OK', {'colspan': 1, 'classes': 'col-US-OK header_final'}), ('REPEATED CN (all)', {'colspan': 1, 'classes': 'col-CN header_final'}), 
                                          ('REPEATED CN-04', {'colspan': 1, 'classes': 'col-CN-04 header_final'}), ('REPEATED CN-11', {'colspan': 1, 'classes': 'col-CN-11 header_final'}), 
                                          ('REPEATED SG', {'colspan': 1, 'classes': 'col-SG header_final'}), ('REPEATED AE', {'colspan': 1, 'classes': 'col-AE header_final'}), 
                                          ('REPEATED GB', {'colspan': 1, 'classes': 'col-GB header_final'})])

        
    def test_filters_tree(self):
        data = {
            "k1": {
                "k11": {
                    "k111": ["v3", "v2","v1"],
                    "k112": ["v4", "v5"]
                },
                "k12": ["v9", "v8"]
            }
        }
        filtered = filtered_hierarchy(data,["v8","v9","v1","v3","v5"])
        assert filtered ==  {'k1': {'k11': {
                                        'k111': ['v1', 'v3'], 
                                        'k112': ['v5']}, 
                                    'k12': ['v8', 'v9']}}

    def test_calculates_width(self):
        data = {
            "k1": {
                "k11": {
                    "k111": ["v3", "v2","v1"],
                    "k112": ["v4", "v5"]
                },
                "k12": ["v9", "v8"]
            }
        }  
        widths = hierarchy_widths(data)
        assert widths == {'k1':7, 'k1#k12':2, 'k1#k11':5, 'k1#k11#k111': 3, 'k1#k11#k112':2}

class TestFamilyTree(TestCase):

    def setUpTestData():
        clean_db()
        org_nodes = [make_node(x,y) for x,y in zip(range(100,200),"abcdefghijklmnz")]
        org_nodes = org_nodes + [make_node(x,y) for x,y in zip(range(200,210),["p1","p2","s1","s2","c1","c2","p3"])]
        org_nodes = sorted(org_nodes, reverse=True)
        act_nodes = [make_node(x,y,"CorporateFinanceActivity") for x,y in zip(range(100,200),"opqrstuvw")]
        act_nodes = act_nodes + [make_node(x,y,"CorporateFinanceActivity") for x,y in zip(range(200,210),["a1","a2","a3","a4"])]
        node_list = ", ".join(org_nodes + act_nodes)
        query = f"""CREATE {node_list},
            (a)-[:buyer]->(q)-[:target]->(b),
            (a)-[:investor]->(r)-[:target]->(c),
            (b)-[:buyer]->(s)-[:target]->(d),

            (a)-[:sameAsNameOnly]->(e),
            (e)-[:buyer]->(o)-[:target]->(f),
            (b)-[:sameAsNameOnly]->(g),
            (g)-[:investor]->(p)-[:target]->(h),
            (z)-[:sameAsNameOnly]->(d),

            (i)-[:buyer]->(w)-[:target]->(j),
            (i)-[:investor]->(t)-[:target]->(k),
            (j)-[:buyer]->(u)-[:target]->(l),

            (l)-[:sameAsNameOnly]->(n),
            (m)-[:buyer]->(v)-[:target]->(n),

            (p1)-[:buyer]->(a1)-[:target]->(s1),
            (s1)-[:sameAsNameOnly]->(s2),
            (s2)-[:buyer]->(a2)-[:target]->(c1),
            (p1)-[:sameAsNameOnly]->(p2),
            (p2)-[:buyer]->(a3)-[:target]->(c2),
            (p3)-[:buyer]->(a4)-[:target]->(s1)
        """
        db.cypher_query(query)

    def setUp(self):
        ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{ts}")

    def test_gets_parent_orgs_without_same_as_name_only(self):
        uri = "https://1145.am/db/111/l"
        parents = get_parent_orgs(uri,combine_same_as_name_only=False)
        assert len(parents) == 1
        uris = [x.uri for x,_,_,_,_,_ in parents]
        assert set(uris) == set(["https://1145.am/db/109/j"])

    def test_gets_parent_orgs_with_same_as_name_only(self):
        uri = "https://1145.am/db/111/l"
        parents = get_parent_orgs(uri,combine_same_as_name_only=True)
        assert len(parents) == 2
        uris = [x.uri for x,_,_,_,_,_ in parents]
        assert set(uris) == set(["https://1145.am/db/109/j", "https://1145.am/db/112/m"])

    def test_gets_child_orgs_without_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        children = get_child_orgs(uri,combine_same_as_name_only=False)
        assert len(children) == 1
        child_uris = [x.uri for (_,x,_,_,_,_) in children]
        assert set(child_uris) == set(["https://1145.am/db/103/d"])

    def test_gets_child_orgs_with_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        children = get_child_orgs(uri,combine_same_as_name_only=True)
        assert len(children) == 2
        child_uris = [x.uri for (_,x,_,_,_,_) in children]
        assert set(child_uris) == set(["https://1145.am/db/103/d","https://1145.am/db/107/h"])

    def test_gets_family_tree_without_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        o = Organization.self_or_ultimate_target_node(uri)
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":False,
                                                    "relationship_str":"buyer,vendor,investor"})
        d = nodes_edges.data
        assert len(d['nodes']) == 4
        assert len(d['edges']) == 3
        edge_details = json.loads(d['edge_details'])
        assert len(edge_details) == len(d['edges'])
        node_details = json.loads(d['node_details'])
        assert len(node_details) == len(d['nodes'])

    def test_gets_family_tree_with_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        o = Organization.self_or_ultimate_target_node(uri)
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":True,
                                                       "relationship_str":"buyer,vendor,investor"})
        d = nodes_edges.data
        assert len(d['nodes']) == 6
        assert len(d['edges']) == 5
        edge_details = json.loads(d['edge_details'])
        assert len(edge_details) == len(d['edges'])
        node_details = json.loads(d['node_details'])
        assert len(node_details) == len(d['nodes'])

    def test_gets_children_where_there_is_no_parent(self):
        uri = "https://1145.am/db/100/a"
        o = Organization.self_or_ultimate_target_node(uri)
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":True,
                                                         "relationship_str":"buyer,vendor,investor"})
        d = nodes_edges.data
        names = [x['label'] for x in d['nodes']]
        assert set(names) == set(["Name A","Name B","Name C","Name F"])

    def test_shows_nodes_in_name_order(self):
        client = self.client
        path = "/organization/family-tree/uri/1145.am/db/101/b?rels=buyer,vendor,investor&earliest_date=-1"
        response = client.get(path)
        assert response.status_code == 403
        client.force_login(self.user)
        response = client.get(path)
        assert response.status_code == 200
        content = str(response.content)
        res = re.search(r"var edges = new vis.DataSet\( (.+?) \);",content)
        as_dict = json.loads(res.groups(0)[0].replace("\\'","\""))
        target_ids = [x['to'] for x in as_dict]
        assert target_ids == ['https://1145.am/db/101/b', 'https://1145.am/db/102/c', 
                              'https://1145.am/db/105/f', 'https://1145.am/db/103/d', 
                              'https://1145.am/db/107/h']

    def test_links_parent_and_child_if_only_linked_via_same_as_name_only(self):
        client = self.client
        path = "/organization/family-tree/uri/1145.am/db/202/s1?rels=buyer,vendor&earliest_date=-1&combine_same_as_name_only=1"
        client.force_login(self.user)
        response = client.get(path)
        content = str(response.content)
        assert "https://1145.am/db/200/p1-buyer-https://1145.am/db/202/s1" in content # link from p1 to s1
        assert "https://1145.am/db/202/s1-buyer-https://1145.am/db/204/c1" in content # link from s1 to c1 (but it was s2 who bought c1)

    def test_switches_sibling_if_parent_has_different_same_as_child(self):
        '''
        Looking for s2, which is sameAsNameOnly with s1, so there shouldn't be any reference to s1 here
        '''
        client = self.client
        client.force_login(self.user)
        response = client.get("/organization/family-tree/uri/1145.am/db/203/s2?earliest_date=-1")
        content = str(response.content)
        assert "https://1145.am/db/200/p1-buyer-https://1145.am/db/202/s1" not in content # link from p1 to s1
        assert "https://1145.am/db/200/p1-buyer-https://1145.am/db/203/s2" in content # link from s1 to c1 (but it was s2 who bought c1)

    def test_switches_sibling_if_different_parent_has_same_as_name_only_child(self):
        client = self.client
        client.force_login(self.user)
        response = client.get("/organization/family-tree/uri/1145.am/db/203/s2?earliest_date=-1")
        content = str(response.content)
        assert "https://1145.am/db/200/p1-buyer-https://1145.am/db/202/s1" not in content # link from p1 to s1
        assert "https://1145.am/db/200/p1-buyer-https://1145.am/db/203/s2" in content # link from s1 to c1 (but it was s2 who bought c1)

    def test_updates_sibling_target_if_central_org_is_linked_by_same_only(self):
        client = self.client
        client.force_login(self.user)
        response = client.get("/organization/family-tree/uri/1145.am/db/202/s1?rels=buyer,vendor&earliest_date=-1&combine_same_as_name_only=1")
        content = str(response.content)
        # p3 bought s2, but I'm looking at s1 which has sameAsNameOnly with s2, so should only be seeing s1
        assert 'https://1145.am/db/206/p3-buyer-https://1145.am/db/203/s2' not in content
        assert 'https://1145.am/db/206/p3-buyer-https://1145.am/db/202/s1' in content 

    def test_links_multiple_parents_to_same_child(self):
        client = self.client
        client.force_login(self.user)
        response = client.get("/organization/family-tree/uri/1145.am/db/202/s1")
        content = str(response.content)
        assert 'https://1145.am/db/206/p3-buyer-https://1145.am/db/202/s1' in content


class TestDynamicClasses(TestCase):

    def test_can_pickle_dynamic_class(self):
        # Needed for caching in redis
        ts = time.time()
        uri = f"https://example.org/foo/bar/{ts}"
        query = f"CREATE (n: Resource&Person&Organization {{uri:'{uri}'}})"
        db.cypher_query(query)
        class_factory("OrganizationPerson",(Organization, Person, Resource))
        obj = Resource.nodes.get_or_none(uri=uri)
        res = pickle.dumps(obj)
        unpickled = pickle.loads(res)
        assert obj == unpickled

class TestSerializers(TestCase):

    def test_cleans_relationship_string1(self):
        val = only_valid_relationships("<foobar>vendor</foobar>")
        assert val == "buyer|vendor"

    def test_cleans_relationship_string2(self):
        val = only_valid_relationships("investor|buyer|vendor")
        assert val == "investor|buyer|vendor"


class TestFindResultsArticleCounts(TestCase):

    def setUpTestData():
        clean_db()
        one_year_ago = datetime.now() - timedelta(365)
        five_years_ago = datetime.now() - timedelta(365 * 5)
        node_data = [
            {"doc_id":100,"identifier":"foo_new_one","datestamp": one_year_ago},
            {"doc_id":101,"identifier":"foo_old_one","datestamp": five_years_ago},
            {"doc_id":102,"identifier":"bar_new_one","datestamp": one_year_ago},
            {"doc_id":103,"identifier":"bar_old_one","datestamp": five_years_ago},
            {"doc_id":200,"identifier":"same_as_one","datestamp": one_year_ago},
            {"doc_id":201,"identifier":"same_as_two","datestamp": one_year_ago},
            {"doc_id":202,"identifier":"same_as_three","datestamp": five_years_ago},
            {"doc_id":203,"identifier":"same_as_four","datestamp": one_year_ago},
            {"doc_id":204,"identifier":"same_as_five","datestamp": five_years_ago},
            {"doc_id":300,"identifier":"same_as_b_one","datestamp": one_year_ago},
            {"doc_id":301,"identifier":"same_as_b_two","datestamp": five_years_ago},
            {"doc_id":302,"identifier":"same_as_b_three","datestamp": one_year_ago},
        ]

        nodes = [make_node(**data) for data in node_data]
        node_list = ", ".join(nodes)
        query = f"""
            CREATE {node_list},
            (ind1: Resource:IndustryCluster {{uri:"https://1145.am/ind1", topicId: 23, representativeDoc:['paper printing','cardboard packaging']}}),
            (ind2: Resource:IndustryCluster {{uri:"https://1145.am/ind2", topicId: 24, representativeDoc:['computers','server hardware']}}),
            (loc1: Resource:GeoNamesLocation {{uri:"https://1145.am/loc1", geoNamesId: 4509884}}), // In US-Ohio
            (loc2: Resource:GeoNamesLocation {{uri:"https://1145.am/loc2", geoNamesId: 4791259}}), // In US-Virginia 
            
            (foo_new_one)-[:industryClusterPrimary]->(ind1),
            (foo_old_one)-[:industryClusterPrimary]->(ind1),
            (foo_new_one)-[:basedInHighGeoNamesLocation]->(loc1),
            (bar_new_one)-[:basedInHighGeoNamesLocation]->(loc1),
            (bar_old_one)-[:basedInHighGeoNamesLocation]->(loc1),
            (loc1)-[:geoNamesURL]->(:Resource {{uri:"https://sws.geonames.org/4509884",sourceOrganization:"source_org_foo"}}),
            (loc2)-[:geoNamesURL]->(:Resource {{uri:"https://sws.geonames.org/4791259",sourceOrganization:"source_org_foo"}}),

            (same_as_one)-[:sameAsNameOnly]->(same_as_two),
            (same_as_one)-[:sameAsNameOnly]->(same_as_three),
            (same_as_one)-[:sameAsNameOnly]->(same_as_four),
            (same_as_one)-[:sameAsNameOnly]->(same_as_five),
            (same_as_two)-[:sameAsNameOnly]->(same_as_three),
            (same_as_two)-[:sameAsNameOnly]->(same_as_four),
            (same_as_two)-[:sameAsNameOnly]->(same_as_five),
            (same_as_three)-[:sameAsNameOnly]->(same_as_four),
            (same_as_three)-[:sameAsNameOnly]->(same_as_five),
            (same_as_four)-[:sameAsNameOnly]->(same_as_five),

            (same_as_b_one)-[:sameAsNameOnly]->(same_as_b_two),
            (same_as_b_one)-[:sameAsNameOnly]->(same_as_b_three),
            (same_as_b_two)-[:sameAsNameOnly]->(same_as_b_three)
        """
        db.cypher_query(query)
        RDFPostProcessor().run_all_in_order()
        refresh_geo_data()

    def search_by_name_check_counts(self):
        min_date = datetime.now() - timedelta(365 * 2)
        res = Organization.find_by_name("foo",True,min_date)
        sorted_res = sorted(res, key=lambda x: x[1],reverse=True)
        assert sorted_res[0][0].uri == 'https://1145.am/db/100/foo_new_one'
        assert sorted_res[0][1] == 1
        assert sorted_res[1][0].uri == 'https://1145.am/db/101/foo_old_one'
        assert sorted_res[1][1] == 0 # Doc is more than 2 years old
        assert 1 == 2

    def search_by_geo_counts(self):
        min_date = datetime.now() - timedelta(365 * 2)
        res = orgs_by_industry_and_or_geo(None, 'US-OH', min_date=min_date)
        check_org_and_counts(res, 
                [ ('https://1145.am/db/102/bar_new_one',1),
                    ('https://1145.am/db/100/foo_new_one',1),
                    ('https://1145.am/db/103/bar_old_one',0),
                ])

    def search_by_industry_counts(self):
        min_date = datetime.now() - timedelta(365 * 2)
        res = orgs_by_industry_and_or_geo(23, None, min_date=min_date)
        check_org_and_counts(res,
            [('https://1145.am/db/101/foo_old_one', 0), 
             ('https://1145.am/db/100/foo_new_one', 1)])
        
    def search_by_industry_geo_counts(self):
        min_date = datetime.now() - timedelta(365 * 2)
        res = orgs_by_industry_and_or_geo(23, 'US-OH', min_date=min_date)
        check_org_and_counts(res,
            [('https://1145.am/db/100/foo_new_one', 1)])
        
    def search_by_same_as_name_only(self):
        min_date =  datetime.now() - timedelta(365 * 2)
        res = Organization.find_by_name("same",combine_same_as_name_only=True,min_date=min_date)
        check_org_and_counts(res,
            [('https://1145.am/db/300/same_as_b_one', 2), 
             ('https://1145.am/db/200/same_as_one', 3)]
        )

class TestIgnoreLowRelativeWeightGeoLocations(TestCase):

    def setUpTestData():
        '''
            Based on
            CALL apoc.export.cypher.query( 
            'match (o: Resource&Organization)-[b:basedInHighGeoNamesLocation]-(l: Resource&GeoNamesLocation), (o)-[:documentSource]->(a: Resource&Article), (l)-[:documentSource]->(a) where o.uri in ["https://1145.am/db/88496/Exelixis_Inc","https://1145.am/db/90949/Moderna","https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"] return *',
            null,
            {format: "plain", stream:true})
        '''
        clean_db()
        queries = """CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (node:Resource) REQUIRE (node.uri) IS UNIQUE;
            UNWIND [{uri:"https://1145.am/db/geonames_location/4930956", properties:{featureCode:"PPLA", deletedRedundantSameAsAt:1, countryCode:"US", name:["Boston"], geoNamesId:4930956, admin1Code:"MA"}}, {uri:"https://1145.am/db/geonames_location/5380748", properties:{featureCode:"PPL", deletedRedundantSameAsAt:1, countryCode:"US", name:["Palo Alto"], geoNamesId:5380748, admin1Code:"CA"}}, {uri:"https://1145.am/db/geonames_location/4464368", properties:{featureCode:"PPLA2", deletedRedundantSameAsAt:1, countryCode:"US", name:["Durham"], geoNamesId:4464368, admin1Code:"NC"}}, {uri:"https://1145.am/db/geonames_location/3042142", properties:{featureCode:"PCL", deletedRedundantSameAsAt:1, countryCode:"JE", name:["Bailiwick of Jersey"], geoNamesId:3042142, admin1Code:"00"}}, {uri:"https://1145.am/db/geonames_location/2643743", properties:{featureCode:"PPLC", deletedRedundantSameAsAt:1, countryCode:"GB", name:["London"], geoNamesId:2643743, admin1Code:"ENG"}}, {uri:"https://1145.am/db/geonames_location/6251999", properties:{featureCode:"PCLI", deletedRedundantSameAsAt:1, countryCode:"CA", name:["Canada"], geoNamesId:6251999, admin1Code:"00"}}, {uri:"https://1145.am/db/geonames_location/2077456", properties:{featureCode:"PCLI", deletedRedundantSameAsAt:1, countryCode:"AU", name:["Commonwealth of Australia"], geoNamesId:2077456, admin1Code:"00"}}, {uri:"https://1145.am/db/geonames_location/6252001", properties:{featureCode:"PCLI", deletedRedundantSameAsAt:1, countryCode:"US", name:["United States"], geoNamesId:6252001, admin1Code:"00"}}, {uri:"https://1145.am/db/geonames_location/1645457", properties:{featureCode:"PPLC", deletedRedundantSameAsAt:1, countryCode:"TL", name:["Dili"], geoNamesId:1645457, admin1Code:"DI"}}, {uri:"https://1145.am/db/geonames_location/5391959", properties:{featureCode:"PPLA2", deletedRedundantSameAsAt:1, countryCode:"US", name:["San Francisco"], geoNamesId:5391959, admin1Code:"CA"}}, {uri:"https://1145.am/db/geonames_location/5128581", properties:{featureCode:"PPL", deletedRedundantSameAsAt:1, countryCode:"US", name:["New York City"], geoNamesId:5128581, admin1Code:"NY"}}, {uri:"https://1145.am/db/geonames_location/2635167", properties:{featureCode:"PCLI", deletedRedundantSameAsAt:1, countryCode:"GB", name:["United Kingdom of Great Britain and Northern Ireland"], geoNamesId:2635167, admin1Code:"00"}}, {uri:"https://1145.am/db/geonames_location/3993763", properties:{featureCode:"PPLA2", deletedRedundantSameAsAt:1, countryCode:"MX", name:["Palo Alto"], geoNamesId:3993763, admin1Code:"01"}}, {uri:"https://1145.am/db/geonames_location/5397765", properties:{featureCode:"PPL", deletedRedundantSameAsAt:1, countryCode:"US", name:["South San Francisco"], geoNamesId:5397765, admin1Code:"CA"}}, {uri:"https://1145.am/db/geonames_location/2964574", properties:{featureCode:"PPLC", deletedRedundantSameAsAt:1, countryCode:"IE", name:["Dublin"], geoNamesId:2964574, admin1Code:"L"}}, {uri:"https://1145.am/db/geonames_location/6254926", properties:{featureCode:"ADM1", deletedRedundantSameAsAt:1, countryCode:"US", name:["Massachusetts"], geoNamesId:6254926, admin1Code:"MA"}}, {uri:"https://1145.am/db/geonames_location/4931972", properties:{featureCode:"PPL", deletedRedundantSameAsAt:1, countryCode:"US", name:["Cambridge"], geoNamesId:4931972, admin1Code:"MA"}}, {uri:"https://1145.am/db/geonames_location/2963597", properties:{featureCode:"PCLI", deletedRedundantSameAsAt:1, countryCode:"IE", name:["Ireland"], geoNamesId:2963597, admin1Code:"00"}}, {uri:"https://1145.am/db/geonames_location/5322737", properties:{featureCode:"PPL", deletedRedundantSameAsAt:1, countryCode:"US", name:["Alameda"], geoNamesId:5322737, admin1Code:"CA"}}, {uri:"https://1145.am/db/geonames_location/4192205", properties:{featureCode:"PPLA2", deletedRedundantSameAsAt:1, countryCode:"US", name:["Dublin"], geoNamesId:4192205, admin1Code:"GA"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:GeoNamesLocation;
            UNWIND [{uri:"https://1145.am/db/geonames_location/4887398", properties:{featureCode:"PPLA2", deletedRedundantSameAsAt:1, countryCode:"US", name:["Chicago"], geoNamesId:4887398, admin1Code:"IL"}}, {uri:"https://1145.am/db/geonames_location/4158928", properties:{featureCode:"PPL", deletedRedundantSameAsAt:1, countryCode:"US", name:["Hollywood"], geoNamesId:4158928, admin1Code:"FL"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:GeoNamesLocation;
            UNWIND [{uri:"https://1145.am/db/88496/Exelixis_Inc", properties:{basedInHighClean:["Alameda"], internalDocId:88496, deletedRedundantSameAsAt:1, foundName:["Exelixis Inc", "Exelixis"], name:["Exelixis Inc"], description:["Pharmaceuticals"], industry:["drug developer"], basedInLowRaw:["United States"], basedInHighRaw:["Alameda, California"]}}, {uri:"https://1145.am/db/90949/Moderna", properties:{internalDocId:90949, deletedRedundantSameAsAt:1, foundName:["Moderna, Inc", "Moderna"], name:["Moderna"], description:["Biotechnology"], industry:["biotechnology"], internalMergedSameAsHighToUri:"https://1145.am/db/87262/Moderna"}}, {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc", properties:{basedInHighClean:["Dublin"], internalDocId:88387, deletedRedundantSameAsAt:1, foundName:["Jazz", "Jazz Pharmaceuticals PLC"], name:["Jazz Pharmaceuticals PLC"], industry:["Pharmaceuticals"], basedInLowRaw:["United States"], basedInHighRaw:["Dublin"]}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Organization;
            UNWIND [{uri:"https://1145.am/db/2370560/wwwfiercebiotechcom_biotech_exelixis-exel-announces-key-senior-leadership-hires-medical-affairs-sales-and-marketing-to", properties:{datePublished:datetime('2015-09-25T12:53:31Z'), internalDocId:2370560, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis (EXEL) Announces Key Senior Leadership Hires In Medical Affairs, Sales, And Marketing To Support Commercialization Of Cabozantinib And Cobimetinib"}}, {uri:"https://1145.am/db/934888/apnewscom_press-release_GlobeNewswire_business-health-mental-health-sleep-apnea-migraine-b9a089b7a81d5a0cb805c292fd947a1d", properties:{datePublished:datetime('2022-03-28T10:02:44Z'), internalDocId:934888, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Axsome Therapeutics to Acquire Sunosi® from Jazz Pharmaceuticals, Expanding Axsome's Leadership in Neuroscience"}}, {uri:"https://1145.am/db/3567722/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-appointment-of-anne-oriordan-to-its-board-of-directors-300796202html", properties:{datePublished:datetime('2019-02-14T21:05:00Z'), internalDocId:3567722, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T19:35:10Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Appointment of Anne O'Riordan to its Board of Directors"}}, {uri:"https://1145.am/db/2557428/wwwfiercepharmacom_pharma_gearing-up-for-commercial-ops-moderna-recruits-amgen-vet-meline-as-cfo", properties:{datePublished:datetime('2020-06-04T15:14:58Z'), internalDocId:2557428, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Gearing up for commercial ops, Moderna recruits Amgen vet Meline as CFO"}}, {uri:"https://1145.am/db/1042008/apnewscom_press-release_GlobeNewswire_business-health-sleep-disorders-apnea-narcolepsy-f2b39aed5c017a2b7b9f1b33232980b9", properties:{datePublished:datetime('2022-05-09T13:02:56Z'), internalDocId:1042008, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Axsome Therapeutics Completes U.S. Acquisition of Sunosi® (solriamfetol) for Excessive Daytime Sleepiness Associated with Narcolepsy or Obstructive Sleep Apnea"}}, {uri:"https://1145.am/db/2563661/wwwfiercepharmacom_pharma_moderna-lures-novartis-top-lawyer-to-be-its-own-as-covid-19-vaccine-rollout-raises-legal", properties:{datePublished:datetime('2021-03-08T16:00:00Z'), internalDocId:2563661, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Moderna lures Novartis' top lawyer to be its own as COVID-19 vaccine rollout raises legal risks"}}, {uri:"https://1145.am/db/1514071/apnewscom_press-release_accesswire_health-business-covid-5891a8e2ea0496fdef99b0054c400e6e", properties:{datePublished:datetime('2022-11-17T13:11:36Z'), internalDocId:1514071, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"David Jiménez Joins Moderna As U.S. General Manager"}}, {uri:"https://1145.am/db/3717517/seekingalphacom_news_4045416-moderna-ceo-lead-sales-commercial-chief-departs", properties:{datePublished:datetime('2023-12-12T13:27:09Z'), internalDocId:3717517, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T12:26:58Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna CEO to lead sales as commercial chief departs"}}, {uri:"https://1145.am/db/3640637/seekingalphacom_news_4041193-generation-bio-announces-layoffs-business-reorganization", properties:{datePublished:datetime('2023-11-29T15:16:55Z'), internalDocId:3640637, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T07:58:01Z'), sourceOrganization:"Seeking Alpha", headline:"Generation Bio cuts 40% of staff in business reorganization"}}, {uri:"https://1145.am/db/4236941/seekingalphacom_news_4088927-moderna-pauses-plans-kenya-vaccine-plant", properties:{datePublished:datetime('2024-04-11T13:24:54Z'), internalDocId:4236941, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-05-13T12:14:26Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna pauses plans for Kenya vaccine plant (NASDAQ:MRNA)"}}, {uri:"https://1145.am/db/2319682/wwwfiercebiotechcom_biotech_press-release-jazz-pharmaceuticals-announces-ipo", properties:{datePublished:datetime('2007-06-01T15:07:56Z'), internalDocId:2319682, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Press release: Jazz Pharmaceuticals Announces IPO"}}, {uri:"https://1145.am/db/2351610/wwwfiercebiotechcom_biotech_jazz-pharma-basks-praise-as-it-adds-cns-drugs-azur-acquisition", properties:{datePublished:datetime('2011-09-20T15:42:48Z'), internalDocId:2351610, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharma basks in praise as it adds CNS drugs in Azur acquisition"}}, {uri:"https://1145.am/db/2344386/wwwfiercebiotechcom_r-d_exelixis-tanks-on-major-stock-and-debt-sales", properties:{datePublished:datetime('2012-08-07T15:59:25Z'), internalDocId:2344386, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis tanks on major stock and debt sales"}}, {uri:"https://1145.am/db/2354353/wwwfiercebiotechcom_r-d_biotech-upstart-moderna-nails-down-40m-for-bold-rna-idea", properties:{datePublished:datetime('2012-12-06T05:03:44Z'), internalDocId:2354353, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Biotech upstart Moderna nails down $40M for bold RNA idea"}}, {uri:"https://1145.am/db/2155125/wwwfiercebiotechcom_biotech_moderna-shoots-for-500m-ipo-biggest-biotech-history", properties:{datePublished:datetime('2018-11-10T00:55:05Z'), internalDocId:2155125, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Moderna shoots for $500M IPO, the biggest in biotech history"}}, {uri:"https://1145.am/db/2154688/wwwfiercebiotechcom_biotech_moderna-s-cash-juggernaut-rolls-record-604m-ipo", properties:{datePublished:datetime('2018-12-07T14:07:49Z'), internalDocId:2154688, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Moderna's cash juggernaut rolls on with record $604M IPO"}}, {uri:"https://1145.am/db/3060848/wwwcityamcom_david-beckham-backed-cannabis-firm-on-course-to-become-lses-first-cbd-listing", properties:{datePublished:datetime('2021-02-04T13:00:00Z'), internalDocId:3060848, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2023-12-20T05:41:42Z'), sourceOrganization:"CityAM", headline:"David Beckham-backed cannabis firm on course to become LSE's first CBD listing"}}, {uri:"https://1145.am/db/2329765/wwwfiercebiotechcom_biotech_press-release-exelixis-signs-co-development-agreement-genentech", properties:{datePublished:datetime('2007-01-03T15:25:26Z'), internalDocId:2329765, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Press Release: Exelixis Signs Co-Development Agreement With Genentech"}}, {uri:"https://1145.am/db/2328316/wwwfiercebiotechcom_biotech_antares-jazz-ink-16-5m-cns-deal", properties:{datePublished:datetime('2007-07-20T10:59:54Z'), internalDocId:2328316, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Antares, Jazz ink $16.5M CNS deal"}}, {uri:"https://1145.am/db/2301942/wwwfiercebiotechcom_biotech_press-release-glaxosmithkline-accelerates-review-of-exelixis-xl880", properties:{datePublished:datetime('2007-08-23T13:11:01Z'), internalDocId:2301942, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"PRESS RELEASE: GlaxoSmithKline Accelerates Review of Exelixis' XL880"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/2307047/wwwfiercebiotechcom_biotech_press-release-exelixis-and-bristol-myers-squibb-extend-research-deal", properties:{datePublished:datetime('2007-09-21T15:29:28Z'), internalDocId:2307047, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"PRESS RELEASE: Exelixis and Bristol-Myers Squibb Extend Research Deal"}}, {uri:"https://1145.am/db/2324466/wwwfiercebiotechcom_biotech_press-release-exelixis-to-receive-milestone-payment-from-bristol-myers-squibb", properties:{datePublished:datetime('2007-11-29T17:57:35Z'), internalDocId:2324466, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"PRESS RELEASE: Exelixis to Receive Milestone Payment From Bristol-Myers Squibb"}}, {uri:"https://1145.am/db/2329868/wwwfiercebiotechcom_biotech_glaxosmithkline-via-its-center-of-excellence-for-external-drug-discovery-exercises-its", properties:{datePublished:datetime('2007-12-14T15:18:54Z'), internalDocId:2329868, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"GlaxoSmithKline, Via Its Center of Excellence for External Drug Discovery, Exercises Its Option to Further Develop and Commercia"}}, {uri:"https://1145.am/db/2305680/wwwfiercebiotechcom_biotech_exelixis-and-glaxosmithkline-agree-to-successfully-conclude-six-year-discovery-and", properties:{datePublished:datetime('2008-06-27T14:59:33Z'), internalDocId:2305680, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis and GlaxoSmithKline Agree to Successfully Conclude Six-Year Discovery and Development Collaboration"}}, {uri:"https://1145.am/db/2302187/wwwfiercebiotechcom_biotech_exelixis-says-glaxosmithkline-collaboration-to-end", properties:{datePublished:datetime('2008-10-23T14:37:39Z'), internalDocId:2302187, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Says GlaxoSmithKline Collaboration to End"}}, {uri:"https://1145.am/db/2304714/wwwfiercebiotechcom_biotech_exelixis-research-collaboration-bristol-myers-squibb-extended", properties:{datePublished:datetime('2008-11-06T15:41:51Z'), internalDocId:2304714, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Research Collaboration With Bristol-Myers Squibb Extended"}}, {uri:"https://1145.am/db/2302181/wwwfiercebiotechcom_biotech_exelixis-retains-rights-to-develop-and-commercialize-xl184", properties:{datePublished:datetime('2008-10-23T14:24:46Z'), internalDocId:2302181, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Retains Rights to Develop and Commercialize XL184"}}, {uri:"https://1145.am/db/2304749/wwwfiercebiotechcom_biotech_exelixis-research-collaboration-bristol-myers-squibb-extended-collaboration-focuses-on", properties:{datePublished:datetime('2008-11-10T13:47:00Z'), internalDocId:2304749, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Research Collaboration With Bristol-Myers Squibb Extended Collaboration Focuses on Novel Cardiovascular, Metabolic Ther"}}, {uri:"https://1145.am/db/2319725/wwwfiercebiotechcom_biotech_exelixis-boehringer-enter-354m-autoimmune-pact", properties:{datePublished:datetime('2009-05-08T13:42:13Z'), internalDocId:2319725, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis, Boehringer enter $354M autoimmune pact"}}, {uri:"https://1145.am/db/2319714/wwwfiercebiotechcom_biotech_exelixis-and-boehringer-ingelheim-enter-into-collaboration-for-development-of-s1p1-receptor", properties:{datePublished:datetime('2009-05-08T12:38:53Z'), internalDocId:2319714, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis and Boehringer Ingelheim Enter into Collaboration for Development of S1P1 Receptor Agonists in the Field of Autoimmune"}}, {uri:"https://1145.am/db/2324063/wwwfiercebiotechcom_biotech_exelixis-and-sanofi-aventis-sign-global-license-agreement-for-xl147-xl765-and-launch-broad", properties:{datePublished:datetime('2009-05-28T11:32:59Z'), internalDocId:2324063, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"EXELIXIS AND SANOFI-AVENTIS SIGN GLOBAL LICENSE AGREEMENT FOR XL147 & XL765 AND LAUNCH BROAD COLLABORATION FOR DISCOVERY OF PI3K"}}, {uri:"https://1145.am/db/2365222/wwwfiercebiotechcom_biotech_exelixis-regains-full-rights-to-develop-and-commercialize-xl184", properties:{datePublished:datetime('2010-06-21T14:47:32Z'), internalDocId:2365222, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Regains Full Rights to Develop and Commercialize XL184"}}, {uri:"https://1145.am/db/2367123/wwwfiercebiotechcom_biotech_exelixis-licenses-programs-to-bristol-myers-squibb-company", properties:{datePublished:datetime('2010-10-11T13:48:25Z'), internalDocId:2367123, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Licenses Programs to Bristol-Myers Squibb Company"}}, {uri:"https://1145.am/db/2350528/wwwfiercebiotechcom_biotech_exelixis-licenses-pi3k-delta-program-to-merck-0", properties:{datePublished:datetime('2011-12-21T12:49:41Z'), internalDocId:2350528, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Licenses PI3K-Delta Program to Merck"}}, {uri:"https://1145.am/db/2350516/wwwfiercebiotechcom_biotech_exelixis-licenses-pi3k-delta-program-to-merck-1", properties:{datePublished:datetime('2011-12-21T11:20:41Z'), internalDocId:2350516, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Licenses PI3K-Delta Program to Merck"}}, {uri:"https://1145.am/db/2351611/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-and-azur-pharma-agree-to-combine-to-form-jazz-pharmaceuticals-plc", properties:{datePublished:datetime('2011-09-20T15:55:24Z'), internalDocId:2351611, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals and Azur Pharma Agree to Combine to Form Jazz Pharmaceuticals plc"}}, {uri:"https://1145.am/db/2367935/wwwfiercebiotechcom_biotech_exelixis-provides-update-on-genentech-s-pending-new-drug-application-for-cobimetinib-an", properties:{datePublished:datetime('2015-07-01T15:03:24Z'), internalDocId:2367935, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Provides Update on Genentech's Pending New Drug Application for Cobimetinib, an Exelixis-Discovered Compound"}}, {uri:"https://1145.am/db/2548593/wwwfiercepharmacom_manufacturing_jazz-pharmaceuticals-breaks-ground-on-new-manufacturing-and-development-facility", properties:{datePublished:datetime('2014-02-10T16:16:35Z'), internalDocId:2548593, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals Breaks Ground On New Manufacturing And Development Facility In Ireland"}}, {uri:"https://1145.am/db/2548596/wwwfiercepharmacom_supply-chain_jazz-investing-up-to-68m-to-build-its-first-manufacturing-facility", properties:{datePublished:datetime('2014-02-10T17:18:15Z'), internalDocId:2548596, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz is investing up to $68M to build its first manufacturing facility"}}, {uri:"https://1145.am/db/3781538/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-and-md-anderson-cancer-center-collaborate-to-evaluate-potential-treatment-options-for-hematologic-malignancies-300692580html", properties:{datePublished:datetime('2018-08-06T20:15:00Z'), internalDocId:3781538, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T17:59:25Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals and MD Anderson Cancer Center Collaborate to Evaluate Potential Treatment Options for Hematologic Malignancies"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/3428795/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-and-codiak-biosciences-announce-strategic-collaboration-to-research-develop-and-commercialize-engineered-exosomes-to-create-therapies-for-hard-to-treat-cancers-300772647html", properties:{datePublished:datetime('2019-01-03T21:05:00Z'), internalDocId:3428795, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T15:57:43Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals and Codiak BioSciences Announce Strategic Collaboration to Research, Develop and Commercialize Engineered Exosomes to Create Therapies for Hard-to-Treat Cancers"}}, {uri:"https://1145.am/db/224813/wwwreuterscom_article_merck-biontech-lipids-idUSL8N2KB29S", properties:{datePublished:datetime('2021-02-05T10:17:20Z'), internalDocId:224813, deletedRedundantSameAsAt:1, sourceOrganization:"Reuters", headline:"Germany's Merck boosts BioNTech lipid supply amid vaccine shortages"}}, {uri:"https://1145.am/db/2358869/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-and-azur-pharma-combine-to-create-jazz-pharmaceuticals-plc", properties:{datePublished:datetime('2012-01-18T17:09:34Z'), internalDocId:2358869, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals and Azur Pharma Combine to Create Jazz Pharmaceuticals plc"}}, {uri:"https://1145.am/db/457686/wwwbusinesswirecom_news_home_20210908005677_en_Resilience-to-Manufacture-mRNA-for-Moderna_E2_80_99s-COVID-19-Vaccine", properties:{datePublished:datetime('2021-09-08T12:13:00Z'), internalDocId:457686, deletedRedundantSameAsAt:1, sourceOrganization:"Business Wire", headline:"Resilience to Manufacture mRNA for Moderna's COVID-19 Vaccine"}}, {uri:"https://1145.am/db/457648/wwwbusinesswirecom_news_home_20210908005443_en_Resilience-to-Manufacture-mRNA-for-Moderna_E2_80_99s-COVID-19-Vaccine", properties:{datePublished:datetime('2021-09-08T12:05:00Z'), internalDocId:457648, deletedRedundantSameAsAt:1, sourceOrganization:"Business Wire", headline:"Resilience to Manufacture mRNA for Moderna's COVID-19 Vaccine"}}, {uri:"https://1145.am/db/2551505/wwwfiercepharmacom_manufacturing_jazz-pharmaceuticals-enters-definitive-agreement-to-acquire-eusa-pharma", properties:{datePublished:datetime('2012-04-26T21:11:12Z'), internalDocId:2551505, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals Enters Definitive Agreement to Acquire EUSA Pharma"}}, {uri:"https://1145.am/db/2353565/wwwfiercebiotechcom_financials_jazz-pharmaceuticals-enters-definitive-agreement-to-acquire-eusa-pharma", properties:{datePublished:datetime('2012-04-27T14:25:35Z'), internalDocId:2353565, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Enters Definitive Agreement to Acquire EUSA Pharma"}}, {uri:"https://1145.am/db/848145/wwwbusinesswirecom_news_home_20220222006242_en_Moderna-and-Thermo-Fisher-Scientific-Announce-Long-Term-Strategic-Collaboration", properties:{datePublished:datetime('2022-02-23T13:00:00Z'), internalDocId:848145, deletedRedundantSameAsAt:1, sourceOrganization:"Business Wire", headline:"Moderna and Thermo Fisher Scientific Announce Long-Term Strategic Collaboration"}}, {uri:"https://1145.am/db/928077/apnewscom_press-release_Accesswire_covid-business-health-australia-melbourne-8aa2bb2fcf051dbd0f91d40cf93ee811", properties:{datePublished:datetime('2022-03-23T23:36:36Z'), internalDocId:928077, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Moderna Finalizes Strategic Partnership with Australian Government"}}, {uri:"https://1145.am/db/1020277/apnewscom_press-release_Accesswire_covid-business-health-canada-pandemics-d04450cc7b4e8c88ecf5e9c56a7e231c", properties:{datePublished:datetime('2022-04-29T14:11:38Z'), internalDocId:1020277, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Moderna Finalizes Plan for Long-Term Strategic Partnership with The Government of Canada"}}, {uri:"https://1145.am/db/1414120/apnewscom_press-release_accesswire_technology-health-cancer-clinical-trials-melanoma-6fff0620621ff0af61560d0e88548877", properties:{datePublished:datetime('2022-10-12T11:16:57Z'), internalDocId:1414120, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Merck and Moderna Announce Exercise of Option by Merck for Joint Development and Commercialization of Investigational Personalized Cancer Vaccine"}}, {uri:"https://1145.am/db/1661887/wwwbusinesswirecom_news_home_20230203005108_en_Personalis-and-Moderna-Sign-New-Agreement-to-Leverage-NeXT-Platform_E2_84_A2-in-Personalized-mRNA-Cancer-Vaccine-Clinical-Trials", properties:{datePublished:datetime('2023-02-03T13:45:00Z'), internalDocId:1661887, deletedRedundantSameAsAt:1, sourceOrganization:"Business Wire", headline:"Personalis and Moderna Sign New Agreement to Leverage NeXT Platform™ in Personalized mRNA Cancer Vaccine Clinical Trials"}}, {uri:"https://1145.am/db/3215326/wwwprnewswirecom_news-releases_ginkgo-bioworks-provides-support-on-process-optimization-to-moderna-for-covid-19-response-301040876html", properties:{datePublished:datetime('2020-04-15T13:00:00Z'), internalDocId:3215326, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-14T00:23:41Z'), sourceOrganization:"PR Newswire", headline:"Ginkgo Bioworks Provides Support on Process Optimization to Moderna for COVID-19 Response"}}, {uri:"https://1145.am/db/2544785/wwwfiercepharmacom_pharma_moderna-taps-national-resilience-s-new-canadian-manufacturing-site-for-covid-19-vaccine", properties:{datePublished:datetime('2021-09-08T15:06:02Z'), internalDocId:2544785, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Moderna taps National Resilience's new Canadian manufacturing site for COVID-19 vaccine production duties"}}, {uri:"https://1145.am/db/3739976/seekingalphacom_news_3923820-moderna-in-pact-for-us-vaccine-contract-targeting-ebola", properties:{datePublished:datetime('2023-01-11T17:07:54Z'), internalDocId:3739976, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T13:45:57Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna in pact for U.S. vaccine contract targeting Ebola (NASDAQ:MRNA)"}}, {uri:"https://1145.am/db/3731989/seekingalphacom_news_4010507-moderna-pact-immatics-cancer-drugs", properties:{datePublished:datetime('2023-09-11T11:24:59Z'), internalDocId:3731989, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T13:18:37Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna in pact with Immatics for cancer drugs (NASDAQ:MRNA)"}}, {uri:"https://1145.am/db/3659594/seekingalphacom_news_4036078-jazz-pharma-enters-collaboration-to-develop-neurological-drugs", properties:{datePublished:datetime('2023-11-14T10:40:42Z'), internalDocId:3659594, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T09:04:26Z'), sourceOrganization:"Seeking Alpha", headline:"Jazz Pharma enters collaboration to develop neurological drugs (NASDAQ:JAZZ)"}}, {uri:"https://1145.am/db/2576103/wwwfiercepharmacom_pharma_jazz-pharmaceuticals-and-gentium-s-p-a-announce-agreement-for-jazz-pharmaceuticals-to", properties:{datePublished:datetime('2013-12-20T13:25:01Z'), internalDocId:2576103, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals And Gentium S.p.A. Announce Agreement For Jazz Pharmaceuticals To Acquire Gentium For $57.00 Per Share"}}, {uri:"https://1145.am/db/2345818/wwwfiercebiotechcom_financials_gentium-shareholders-cry-foul-on-1b-jazz-deal", properties:{datePublished:datetime('2014-01-16T16:05:43Z'), internalDocId:2345818, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Gentium shareholders cry foul on $1B Jazz deal"}}, {uri:"https://1145.am/db/4759477/wwwprnewswirecom_news-releases_oxford-properties-completes-us91-million-sale-of-newly-constructed-140-000-square-foot-boston-gmp-facility-to-pioneering-biotechnology-firm-moderna-301837254html", properties:{datePublished:datetime('2023-05-30T15:00:00Z'), internalDocId:4759477, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-30T02:17:39Z'), sourceOrganization:"PR Newswire", headline:"Oxford Properties Completes US$91 Million Sale of Newly Constructed, 140,000 Square Foot Boston GMP Facility to Pioneering Biotechnology Firm Moderna"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/4815495/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-completes-us-divestiture-of-sunosi-solriamfetol-to-axsome-therapeutics-301542221html", properties:{datePublished:datetime('2022-05-09T11:00:00Z'), internalDocId:4815495, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:41:01Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Completes U.S. Divestiture of Sunosi® (solriamfetol) to Axsome Therapeutics"}}, {uri:"https://1145.am/db/4858126/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-significant-new-investment-in-uk-manufacturing-301510400html", properties:{datePublished:datetime('2022-03-25T07:00:00Z'), internalDocId:4858126, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T22:26:12Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Significant New Investment in UK Manufacturing"}}, {uri:"https://1145.am/db/4819310/wwwprnewswirecom_news-releases_moderna-and-carisma-establish-collaboration-to-develop-in-vivo-engineered-chimeric-antigen-receptor-monocytes-car-m-for-oncology-301456651html", properties:{datePublished:datetime('2022-01-10T11:30:00Z'), internalDocId:4819310, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:54:24Z'), sourceOrganization:"PR Newswire", headline:"Moderna and Carisma Establish Collaboration to Develop in vivo Engineered Chimeric Antigen Receptor Monocytes (CAR-M) for Oncology"}}, {uri:"https://1145.am/db/4845820/wwwprnewswirecom_news-releases_umass-chan-medical-school-announces-research-collaboration-with-moderna-to-examine-impact-of-cytomegalovirus-cmv-in-young-children-301507080html", properties:{datePublished:datetime('2022-03-22T14:00:00Z'), internalDocId:4845820, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T21:32:21Z'), sourceOrganization:"PR Newswire", headline:"UMass Chan Medical School announces research collaboration with Moderna to examine impact of Cytomegalovirus (CMV) in young children"}}, {uri:"https://1145.am/db/4808151/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-and-sumitomo-pharma-announce-exclusive-license-agreement-to-develop-and-commercialize-dsp-0187-a-potent-highly-selective-oral-orexin-2-receptor-agonist-301537690html", properties:{datePublished:datetime('2022-05-04T20:05:00Z'), internalDocId:4808151, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:17:24Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals and Sumitomo Pharma Announce Exclusive License Agreement to Develop and Commercialize DSP-0187, a Potent, Highly Selective Oral Orexin-2 Receptor Agonist"}}, {uri:"https://1145.am/db/2316828/wwwfiercebiotechcom_biotech_press-release-interim-phase-2-data-for-exelixis_C3_A2_E2_82_AC_E2_84_A2-xl880-show-anti-tumor-activity-papillary", properties:{datePublished:datetime('2007-10-25T19:23:33Z'), internalDocId:2316828, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"PRESS RELEASE: INTERIM PHASE 2 DATA FOR EXELIXISâ€™ XL880 SHOW ANTI-TUMOR ACTIVITY IN PAPILLARY RENAL CELL CANCER"}}, {uri:"https://1145.am/db/2316829/wwwfiercebiotechcom_biotech_press-release-exelixis-reports-xl647-clinical-data-at-aacr-nci-eortc-conference", properties:{datePublished:datetime('2007-10-25T19:27:01Z'), internalDocId:2316829, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"PRESS RELEASE: EXELIXIS REPORTS XL647 CLINICAL DATA AT AACR-NCI-EORTC CONFERENCE"}}, {uri:"https://1145.am/db/2310010/wwwfiercebiotechcom_biotech_exelixis-initiates-phase-3-trial-of-xl184-medullary-thyroid-cancer", properties:{datePublished:datetime('2008-07-23T16:12:23Z'), internalDocId:2310010, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Initiates Phase 3 Trial of XL184 in Medullary Thyroid Cancer"}}, {uri:"https://1145.am/db/2366752/wwwfiercebiotechcom_biotech_exelixis-xl184-granted-orphan-drug-designation-and-assigned-generic-name-cabozantinib", properties:{datePublished:datetime('2011-01-10T11:26:20Z'), internalDocId:2366752, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis' XL184 Granted Orphan Drug Designation and Assigned the Generic Name Cabozantinib"}}, {uri:"https://1145.am/db/2347610/wwwfiercebiotechcom_biotech_cabozantinib-xl184-phase-2-data-demonstrate-encouraging-clinical-activity-patients", properties:{datePublished:datetime('2011-02-18T13:42:24Z'), internalDocId:2347610, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Cabozantinib (XL184) Phase 2 Data Demonstrate Encouraging Clinical Activity in Patients with Castration-Resistant Prostate Cance"}}, {uri:"https://1145.am/db/2360947/wwwfiercebiotechcom_biotech_exelixis-reports-positive-preliminary-phase-2-cabozantinib-data-patients-hepatocellular", properties:{datePublished:datetime('2012-01-23T17:39:13Z'), internalDocId:2360947, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Reports Positive Preliminary Phase 2 Cabozantinib Data in Patients with Hepatocellular Carcinoma"}}, {uri:"https://1145.am/db/2364863/wwwfiercebiotechcom_biotech_cabozantinib-shows-encouraging-activity-heavily-pretreated-patients-advanced-renal-cell", properties:{datePublished:datetime('2012-02-03T15:53:31Z'), internalDocId:2364863, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Cabozantinib Shows Encouraging Activity in Heavily Pretreated Patients With Advanced Renal Cell Carcinoma"}}, {uri:"https://1145.am/db/2578118/wwwfiercepharmacom_pharma_jazz-pharmaceuticals-announces-issuance-of-new-formulation-patent-for-xyrem_C2_AE", properties:{datePublished:datetime('2012-09-11T16:33:40Z'), internalDocId:2578118, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals Announces Issuance of New Formulation Patent for Xyrem®"}}, {uri:"https://1145.am/db/2353319/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-begins-clinical-trial-of-intravenously-administered-erwinaze_C2_AE-patients", properties:{datePublished:datetime('2012-12-04T05:00:05Z'), internalDocId:2353319, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Begins Clinical Trial of Intravenously Administered Erwinaze® In Patients with Acute Lymphoblastic Leukemia"}}, {uri:"https://1145.am/db/2354315/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-issuance-of-additional-xyrem_C2_AE-patent", properties:{datePublished:datetime('2012-12-05T05:00:22Z'), internalDocId:2354315, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces Issuance of Additional Xyrem® Patent"}}, {uri:"https://1145.am/db/2351946/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-initiates-rolling-nda-submission-for-defibrotide-for-treatment-of", properties:{datePublished:datetime('2014-12-12T16:30:34Z'), internalDocId:2351946, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Initiates Rolling NDA Submission For Defibrotide For The Treatment Of Severe Hepatic Veno-Occlusive Disease"}}, {uri:"https://1145.am/db/2368452/wwwfiercebiotechcom_biotech_exelixis-announces-positive-top-line-results-from-meteor-phase-3-pivotal-trial-of", properties:{datePublished:datetime('2015-07-20T12:27:55Z'), internalDocId:2368452, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces Positive Top-Line Results from METEOR, the Phase 3 Pivotal Trial of Cabozantinib versus Everolimus in Patients with Metastatic Renal Cell Carcinoma"}}, {uri:"https://1145.am/db/2308891/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-and-ucb-announce-positive-phase-iii-results-for-sodium-oxybate-jzp-6", properties:{datePublished:datetime('2008-11-20T22:20:39Z'), internalDocId:2308891, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals and UCB Announce Positive Phase III Results for Sodium Oxybate (JZP-6) in Fibromyalgia"}}, {uri:"https://1145.am/db/2370702/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-u-s-fda-acceptance-for-filing-priority-review-of-nda-for", properties:{datePublished:datetime('2015-09-30T15:27:07Z'), internalDocId:2370702, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces U.S. FDA Acceptance for Filing with Priority Review of NDA for Defibrotide for Hepatic Veno-Occlusive Disease"}}, {uri:"https://1145.am/db/2368147/wwwfiercebiotechcom_biotech_exelixis-announces-european-commission-approval-of-cotellic_E2_84_A2-cobimetinib-for-use", properties:{datePublished:datetime('2015-11-25T16:15:21Z'), internalDocId:2368147, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces European Commission Approval of COTELLIC™ (Cobimetinib) for Use in Combination with Vemurafenib in Advanced BRAF V600 Mutation-Positive Melanoma"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/2344820/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-acquisition-from-aerial-biopharma-of-rights-to-a-late-stage", properties:{datePublished:datetime('2014-01-14T15:09:31Z'), internalDocId:2344820, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces Acquisition From Aerial Biopharma Of Rights To A Late Stage Investigational Compound For Excessive Daytime Sleepiness"}}, {uri:"https://1145.am/db/2348785/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-agreement-to-acquire-rights-to-defibrotide-americas-from", properties:{datePublished:datetime('2014-07-02T12:45:48Z'), internalDocId:2348785, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces Agreement To Acquire Rights To Defibrotide In The Americas From Sigma-Tau Pharmaceuticals, Inc."}}, {uri:"https://1145.am/db/2314334/wwwfiercebiotechcom_biotech_bristol-myers-squibb-and-exelixis-enter-global-collaboration-on-two-novel-cancer-programs", properties:{datePublished:datetime('2008-12-12T12:55:10Z'), internalDocId:2314334, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Bristol-Myers Squibb and Exelixis Enter Global Collaboration on Two Novel Cancer Programs"}}, {uri:"https://1145.am/db/2370370/wwwfiercebiotechcom_biotech_exelixis-and-ipsen-enter-into-exclusive-licensing-agreement-to-commercialize-and-develop", properties:{datePublished:datetime('2016-03-01T13:08:22Z'), internalDocId:2370370, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis and Ipsen Enter into Exclusive Licensing Agreement to Commercialize and Develop Novel Cancer Therapy Cabozantinib in Regions Outside the United States, Canada and Japan"}}, {uri:"https://1145.am/db/3869856/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-patient-enrolled-in-phase-2-clinical-trial-evaluating-defibrotide-for-the-prevention-of-acute-graft-versus-host-disease-300603339html", properties:{datePublished:datetime('2018-02-23T13:30:00Z'), internalDocId:3869856, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T23:18:21Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Patient Enrolled in Phase 2 Clinical Trial Evaluating Defibrotide for the Prevention of Acute Graft-versus-Host Disease"}}, {uri:"https://1145.am/db/3763144/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-submits-supplemental-new-drug-application-for-xyrem-sodium-oxybate-to-treat-cataplexy-and-excessive-daytime-sleepiness-in-pediatric-narcolepsy-patients-300640005html", properties:{datePublished:datetime('2018-05-01T12:30:00Z'), internalDocId:3763144, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T16:51:57Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Submits Supplemental New Drug Application for Xyrem® (sodium oxybate) to Treat Cataplexy and Excessive Daytime Sleepiness in Pediatric Narcolepsy Patients"}}, {uri:"https://1145.am/db/3934272/wwwprnewswirecom_news-releases_vyxeos-receives-positive-chmp-opinion-for-treatment-of-certain-types-of-high-risk-acute-myeloid-leukaemia-300674611html", properties:{datePublished:datetime('2018-06-29T12:15:00Z'), internalDocId:3934272, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T03:13:34Z'), sourceOrganization:"PR Newswire", headline:"Vyxeos™ Receives Positive CHMP Opinion for Treatment of Certain Types of High-Risk Acute Myeloid Leukaemia"}}, {uri:"https://1145.am/db/2575279/wwwfiercepharmacom_m-a_jazz-pharmaceuticals-announces-another-deal", properties:{datePublished:datetime('2012-04-26T22:06:34Z'), internalDocId:2575279, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals announces another deal"}}, {uri:"https://1145.am/db/4861500/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-agreement-to-divest-sunosi-solriamfetol-to-axsome-therapeutics-301511396html", properties:{datePublished:datetime('2022-03-28T10:00:00Z'), internalDocId:4861500, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T22:42:14Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Agreement to Divest Sunosi® (solriamfetol) to Axsome Therapeutics"}}, {uri:"https://1145.am/db/4815678/wwwprnewswirecom_news-releases_axsome-therapeutics-completes-us-acquisition-of-sunosi-solriamfetol-for-excessive-daytime-sleepiness-associated-with-narcolepsy-or-obstructive-sleep-apnea-301542239html", properties:{datePublished:datetime('2022-05-09T11:48:00Z'), internalDocId:4815678, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:41:33Z'), sourceOrganization:"PR Newswire", headline:"Axsome Therapeutics Completes U.S. Acquisition of Sunosi® (solriamfetol) for Excessive Daytime Sleepiness Associated with Narcolepsy or Obstructive Sleep Apnea"}}, {uri:"https://1145.am/db/3914041/wwwprnewswirecom_news-releases_journal-of-clinical-oncology-publishes-pivotal-phase-3-data-for-jazz-pharmaceuticals-vyxeos-daunorubicin-and-cytarabine-liposome-for-injection-300684026html", properties:{datePublished:datetime('2018-07-19T20:45:00Z'), internalDocId:3914041, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T02:00:03Z'), sourceOrganization:"PR Newswire", headline:"Journal of Clinical Oncology publishes pivotal Phase 3 data for Jazz Pharmaceuticals' Vyxeos® (daunorubicin and cytarabine) Liposome for Injection"}}, {uri:"https://1145.am/db/3914042/wwwprnewswirecom_news-releases_journal-of-clinical-oncology-publishes-pivotal-phase-3-data-for-jazz-pharmaceuticals-vyxeos-688657681html", properties:{datePublished:datetime('2018-07-19T20:51:00Z'), internalDocId:3914042, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T02:00:03Z'), sourceOrganization:"PR Newswire", headline:"Journal of Clinical Oncology publishes pivotal Phase 3 data for Jazz Pharmaceuticals' Vyxeos® (daunorubicin and cytarabine) Liposome for Injection"}}, {uri:"https://1145.am/db/3776206/wwwprnewswirecom_news-releases_cms-grants-new-technology-add-on-payment-to-vyxeos-daunorubicin-and-cytarabine-liposome-for-injection-300691714html", properties:{datePublished:datetime('2018-08-03T12:00:00Z'), internalDocId:3776206, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T17:42:06Z'), sourceOrganization:"PR Newswire", headline:"CMS Grants New Technology Add-On Payment to Vyxeos® (daunorubicin and cytarabine) Liposome for Injection"}}, {uri:"https://1145.am/db/3870222/wwwprnewswirecom_news-releases_vyxeos-receives-marketing-authorisation-in-the-european-union-for-treatment-of-certain-types-of-high-risk-acute-myeloid-leukaemia-300702499html", properties:{datePublished:datetime('2018-08-27T06:00:00Z'), internalDocId:3870222, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T23:19:55Z'), sourceOrganization:"PR Newswire", headline:"Vyxeos® Receives Marketing Authorisation in the European Union for Treatment of Certain Types of High-Risk Acute Myeloid Leukaemia"}}, {uri:"https://1145.am/db/2321635/wwwfiercebiotechcom_biotech_after-holiday-weekend-rumors-jazz-confirms-1-5b-buyout-tiny-celator", properties:{datePublished:datetime('2016-05-31T08:45:35Z'), internalDocId:2321635, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"After holiday weekend rumors, Jazz confirms $1.5B buyout of tiny Celator"}}, {uri:"https://1145.am/db/3001653/wwwcityamcom_ireland-based-jazz-pharmaceuticals-seals-the-deal-with-celator-for-15bn", properties:{datePublished:datetime('2016-05-31T13:14:00Z'), internalDocId:3001653, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2023-12-20T03:59:16Z'), sourceOrganization:"CityAM", headline:"Ireland-based Jazz Pharmaceuticals seals the deal with Celator for $1.5bn tie-up"}}, {uri:"https://1145.am/db/3436539/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-us-fda-approval-of-sunosi-solriamfetol-for-excessive-daytime-sleepiness-associated-with-narcolepsy-or-obstructive-sleep-apnea-300816081html", properties:{datePublished:datetime('2019-03-20T22:53:00Z'), internalDocId:3436539, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T16:10:57Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces U.S. FDA Approval of Sunosi™ (solriamfetol) for Excessive Daytime Sleepiness Associated with Narcolepsy or Obstructive Sleep Apnea"}}, {uri:"https://1145.am/db/3483970/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-positive-top-line-results-from-phase-3-study-of-jzp-258-in-adult-narcolepsy-patients-with-cataplexy-and-excessive-daytime-sleepiness-300819008html", properties:{datePublished:datetime('2019-03-26T20:13:00Z'), internalDocId:3483970, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T17:30:15Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Positive Top-line Results from Phase 3 Study of JZP-258 in Adult Narcolepsy Patients with Cataplexy and Excessive Daytime Sleepiness"}}, {uri:"https://1145.am/db/3919064/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-fda-approval-of-xyrem-sodium-oxybate-for-the-treatment-of-cataplexy-or-excessive-daytime-sleepiness-in-pediatric-narcolepsy-patients-300739288html", properties:{datePublished:datetime('2018-10-29T12:00:00Z'), internalDocId:3919064, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T02:17:41Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces FDA Approval of Xyrem® (sodium oxybate) for the Treatment of Cataplexy or Excessive Daytime Sleepiness in Pediatric Narcolepsy Patients"}}, {uri:"https://1145.am/db/3557631/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-receives-positive-chmp-opinion-for-solriamfetol-to-improve-wakefulness-and-reduce-excessive-daytime-sleepiness-in-adults-with-narcolepsy-or-obstructive-sleep-apnea-300959111html", properties:{datePublished:datetime('2019-11-15T11:45:00Z'), internalDocId:3557631, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T19:18:47Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Receives Positive CHMP Opinion for Solriamfetol to Improve Wakefulness and Reduce Excessive Daytime Sleepiness in Adults with Narcolepsy or Obstructive Sleep Apnea"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/3234395/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-receives-eu-marketing-authorisation-for-sunosi-solriamfetol-for-excessive-daytime-sleepiness-in-adults-with-narcolepsy-or-obstructive-sleep-apnea-300989638html", properties:{datePublished:datetime('2020-01-20T15:35:00Z'), internalDocId:3234395, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-14T00:51:29Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Receives EU Marketing Authorisation for Sunosi® (solriamfetol) for Excessive Daytime Sleepiness in Adults with Narcolepsy or Obstructive Sleep Apnea"}}, {uri:"https://1145.am/db/3248290/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-submits-new-drug-application-for-jzp-258-for-cataplexy-and-excessive-daytime-sleepiness-associated-with-narcolepsy-300991187html", properties:{datePublished:datetime('2020-01-22T13:00:00Z'), internalDocId:3248290, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-14T01:11:26Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Submits New Drug Application for JZP-258 for Cataplexy and Excessive Daytime Sleepiness Associated with Narcolepsy"}}, {uri:"https://1145.am/db/3808173/wwwprnewswirecom_news-releases_national-institute-for-health-and-care-excellence-nice-recommends-jazz-pharmaceuticals-vyxeos-daunorubicin-and-cytarabine-for-adults-with-specific-types-of-secondary-acute-myeloid-leukaemia-aml-300746025html", properties:{datePublished:datetime('2018-11-08T00:05:00Z'), internalDocId:3808173, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T19:36:10Z'), sourceOrganization:"PR Newswire", headline:"National Institute for Health and Care Excellence (NICE) Recommends Jazz Pharmaceuticals' Vyxeos® (Daunorubicin and Cytarabine) for Adults with Specific Types of Secondary Acute Myeloid Leukaemia (AML)"}}, {uri:"https://1145.am/db/3818868/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-submits-marketing-authorization-application-to-european-medicines-agency-for-solriamfetol-as-a-treatment-to-improve-wakefulness-and-reduce-excessive-daytime-sleepiness-in-adult-patients-with-narcolepsy-or-obst-300747377html", properties:{datePublished:datetime('2018-11-09T07:00:00Z'), internalDocId:3818868, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T20:12:53Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Submits Marketing Authorization Application to European Medicines Agency for Solriamfetol as a Treatment to Improve Wakefulness and Reduce Excessive Daytime Sleepiness in Adult Patients with Narcolepsy or Obstructive Sleep Apnea"}}, {uri:"https://1145.am/db/3347551/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-fda-acceptance-of-new-drug-application-for-jzp-258-for-cataplexy-and-excessive-daytime-sleepiness-associated-with-narcolepsy-301029906html", properties:{datePublished:datetime('2020-03-25T20:05:00Z'), internalDocId:3347551, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-14T04:22:53Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces FDA Acceptance of New Drug Application for JZP-258 for Cataplexy and Excessive Daytime Sleepiness Associated with Narcolepsy"}}, {uri:"https://1145.am/db/2295041/wwwfiercebiotechcom_biotech_moderna-gains-barda-zika-vax-funding-closes-474m-funding-round", properties:{datePublished:datetime('2016-09-07T12:05:00Z'), internalDocId:2295041, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Moderna gains BARDA Zika vax funding, closes $474M funding round"}}, {uri:"https://1145.am/db/2552053/wwwfiercepharmacom_vaccines_moderna-prices-covid-19-vaccine-at-32-to-37-for-small-purchasers", properties:{datePublished:datetime('2020-08-05T15:20:00Z'), internalDocId:2552053, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Moderna prices coronavirus shot at up to $37 per dose for small deals. What will big customers get?"}}, {uri:"https://1145.am/db/3149417/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-positive-top-line-results-from-phase-3-study-of-xywav-calcium-magnesium-potassium-and-sodium-oxybates-oral-solution-in-adult-patients-with-idiopathic-hypersomnia-301148512html", properties:{datePublished:datetime('2020-10-08T11:30:00Z'), internalDocId:3149417, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-13T22:52:46Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Positive Top-line Results from Phase 3 Study of Xywav™ (calcium, magnesium, potassium, and sodium oxybates) Oral Solution in Adult Patients with Idiopathic Hypersomnia"}}, {uri:"https://1145.am/db/3178351/wwwprnewswirecom_news-releases_new-data-for-zepzelca-lurbinectedin-to-be-presented-at-iaslc-2020-north-america-conference-on-lung-cancer-301152493html", properties:{datePublished:datetime('2020-10-14T20:05:00Z'), internalDocId:3178351, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-13T23:27:10Z'), sourceOrganization:"PR Newswire", headline:"New Data for Zepzelca™ (lurbinectedin) to be Presented at IASLC 2020 North America Conference on Lung Cancer"}}, {uri:"https://1145.am/db/3199601/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-sleep-publication-of-phase-3-xywav-calcium-magnesium-potassium-and-sodium-oxybates-oral-solution-study-in-cataplexy-or-excessive-daytime-sleepiness-in-patients-with-narcolepsy-301155706html", properties:{datePublished:datetime('2020-10-20T11:30:00Z'), internalDocId:3199601, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-13T23:59:32Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces SLEEP Publication of Phase 3 Xywav™ (calcium, magnesium, potassium, and sodium oxybates) Oral Solution Study in Cataplexy or Excessive Daytime Sleepiness in Patients with Narcolepsy"}}, {uri:"https://1145.am/db/3058456/wwwcityamcom_moderna-seeks-green-light-from-us-and-eu-for-covid-19-vaccine", properties:{datePublished:datetime('2020-11-30T15:50:24Z'), internalDocId:3058456, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2023-12-20T05:37:42Z'), sourceOrganization:"CityAM", headline:"Moderna seeks green light from US and EU for Covid-19 vaccine"}}, {uri:"https://1145.am/db/3918978/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-patient-enrolled-in-phase-3-clinical-trial-evaluating-jzp-258-for-the-treatment-of-idiopathic-hypersomnia-300757548html", properties:{datePublished:datetime('2018-11-29T13:00:00Z'), internalDocId:3918978, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T02:17:19Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Patient Enrolled in Phase 3 Clinical Trial Evaluating JZP-258 for the Treatment of Idiopathic Hypersomnia"}}, {uri:"https://1145.am/db/5064686/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-ceo-succession-plan-302332980html", properties:{datePublished:datetime('2024-12-16T21:05:00Z'), internalDocId:5064686, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-12-17T05:10:27Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces CEO Succession Plan"}}, {uri:"https://1145.am/db/4121340/seekingalphacom_news_4083644-moderna-stock-gains-next-gen-covid-shot-data", properties:{datePublished:datetime('2024-03-26T12:15:09Z'), internalDocId:4121340, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-03-27T08:22:09Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna stock gains on next-gen COVID shot data (NASDAQ:MRNA)"}}, {uri:"https://1145.am/db/4853858/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-patient-enrolled-in-emerge-201-phase-2-basket-trial-evaluating-zepzelca-lurbinectedin-monotherapy-in-patients-with-select-advanced-or-metastatic-solid-tumors-301509864html", properties:{datePublished:datetime('2022-03-24T11:30:00Z'), internalDocId:4853858, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T22:02:29Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Patient Enrolled in EMERGE-201 Phase 2 Basket Trial Evaluating Zepzelca® (lurbinectedin) Monotherapy in Patients with Select Advanced or Metastatic Solid Tumors"}}, {uri:"https://1145.am/db/4756667/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-provides-update-on-phase-2-trial-of-investigational-jzp150-in-adult-patients-with-post-traumatic-stress-disorder-302021299html", properties:{datePublished:datetime('2023-12-21T21:05:00Z'), internalDocId:4756667, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-30T02:05:57Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Provides Update on Phase 2 Trial of Investigational JZP150 in Adult Patients with Post-Traumatic Stress Disorder"}}, {uri:"https://1145.am/db/4935578/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-patient-enrolled-in-phase-2-clinical-trial-evaluating-jzp150-for-once-daily-treatment-of-adults-with-post-traumatic-stress-disorder-301451838html", properties:{datePublished:datetime('2021-12-30T12:45:00Z'), internalDocId:4935578, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-05T09:39:07Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Patient Enrolled in Phase 2 Clinical Trial Evaluating JZP150 for Once-Daily Treatment of Adults with Post-Traumatic Stress Disorder"}}, {uri:"https://1145.am/db/4806958/wwwprnewswirecom_news-releases_us-fda-grants-orphan-drug-exclusivity-ode-for-xywav-calcium-magnesium-potassium-and-sodium-oxybates-oral-solution-for-idiopathic-hypersomnia-in-adults-301452921html", properties:{datePublished:datetime('2022-01-03T21:10:00Z'), internalDocId:4806958, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:13:23Z'), sourceOrganization:"PR Newswire", headline:"U.S. FDA Grants Orphan Drug Exclusivity (ODE) for Xywav® (calcium, magnesium, potassium, and sodium oxybates) Oral Solution for Idiopathic Hypersomnia in Adults"}}, {uri:"https://1145.am/db/4811317/wwwprnewswirecom_news-releases_lancet-neurology-publishes-positive-pivotal-phase-3-data-of-xywav-calcium-magnesium-potassium-and-sodium-oxybates-oral-solution-for-idiopathic-hypersomnia-301454171html", properties:{datePublished:datetime('2022-01-05T12:45:00Z'), internalDocId:4811317, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:27:24Z'), sourceOrganization:"PR Newswire", headline:"Lancet Neurology Publishes Positive, Pivotal Phase 3 Data of Xywav® (calcium, magnesium, potassium, and sodium oxybates) Oral Solution for Idiopathic Hypersomnia"}}, {uri:"https://1145.am/db/1477814/apnewscom_press-release_accesswire_health-business-covid-immunizations-72966aa73cb24c7f2cdd7cf92c62ee6c", properties:{datePublished:datetime('2022-11-04T21:01:55Z'), internalDocId:1477814, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Moderna Receives Health Canada Authorization for Second Omicron-Targeting Bivalent Booster"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/3751907/seekingalphacom_news_3985407-moderna-stock-gains-amid-marketing-submissions-rsv-shot", properties:{datePublished:datetime('2023-07-05T11:31:28Z'), internalDocId:3751907, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T14:26:51Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna stock gains amid marketing submissions for RSV shot"}}, {uri:"https://1145.am/db/4898222/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-present-from-expanding-neuroscience-portfolio-epidiolex-cannabidiol-oral-solution-data-at-the-2021-american-epilepsy-society-annual-meeting-301434871html", properties:{datePublished:datetime('2021-12-01T12:45:00Z'), internalDocId:4898222, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-05T07:16:22Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Present from Expanding Neuroscience Portfolio Epidiolex® (cannabidiol) Oral Solution Data at the 2021 American Epilepsy Society Annual Meeting"}}, {uri:"https://1145.am/db/2301130/wwwfiercebiotechcom_biotech_moderna-raises-500m-to-move-mrna-drugs-deeper-into-human-tests", properties:{datePublished:datetime('2018-02-02T09:17:02Z'), internalDocId:2301130, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Moderna raises $500M to move mRNA drugs deeper into human tests"}}, {uri:"https://1145.am/db/4321683/seekingalphacom_news_4115478-moderna-marks-trial-win-next-gen-covid-shot", properties:{datePublished:datetime('2024-06-13T11:26:13Z'), internalDocId:4321683, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-06-17T12:02:25Z'), sourceOrganization:"Seeking Alpha", headline:"Moderna marks trial win for next-gen COVID shot (NASDAQ:MRNA)"}}, {uri:"https://1145.am/db/4649015/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-presents-updated-phase-2-data-for-zanidatamab-demonstrating-increased-mpfs-in-her2-positive-metastatic-gastroesophageal-adenocarcinoma-at-esmo-2024-302248513html", properties:{datePublished:datetime('2024-09-16T11:30:00Z'), internalDocId:4649015, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-29T18:09:12Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Presents Updated Phase 2 Data for Zanidatamab Demonstrating Increased mPFS in HER2-Positive Metastatic Gastroesophageal Adenocarcinoma at ESMO 2024"}}, {uri:"https://1145.am/db/4994339/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-us-fda-approval-of-ziihera-zanidatamab-hrii-for-the-treatment-of-adults-with-previously-treated-unresectable-or-metastatic-her2-positive-ihc-3-biliary-tract-cancer-btc-302312216html", properties:{datePublished:datetime('2024-11-21T00:19:00Z'), internalDocId:4994339, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-21T05:12:41Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces U.S. FDA Approval of Ziihera® (zanidatamab-hrii) for the Treatment of Adults with Previously Treated, Unresectable or Metastatic HER2-positive (IHC 3+) Biliary Tract Cancer (BTC)"}}, {uri:"https://1145.am/db/5003388/wwwglobenewswirecom_news-release_2024_11_25_2986473_0_en_US-FDA-Grants-Approval-For-Zanidatamab-Ziihera-Bispecific-Antibody-For-Biliary-Tract-Cancerhtml", properties:{datePublished:datetime('2024-11-25T10:47:49Z'), internalDocId:5003388, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-25T20:45:31Z'), sourceOrganization:"GlobeNewswire", headline:"US FDA Grants Approval For Zanidatamab Ziihera Bispecific Antibody For Biliary Tract Cancer"}}, {uri:"https://1145.am/db/2310180/wwwfiercebiotechcom_biotech_also-noted-renovis-drops-ms-compound-lorus-gets-rolling-nda-biotechs-lament-grants-rule-and", properties:{datePublished:datetime('2005-06-13T00:00:50Z'), internalDocId:2310180, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"ALSO NOTED: Renovis drops MS compound, Lorus gets rolling NDA, Biotechs lament grants rule, and much more..."}}, {uri:"https://1145.am/db/2314213/wwwfiercebiotechcom_biotech_press-release-exelixis-reinitiates-clinical-development-of-xl999", properties:{datePublished:datetime('2007-04-24T16:33:00Z'), internalDocId:2314213, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Press Release: Exelixis Reinitiates Clinical Development of XL999"}}, {uri:"https://1145.am/db/2313620/wwwfiercebiotechcom_biotech_press-release-exelixis-diabetic-nephropathy-drug-trial-fails-goal", properties:{datePublished:datetime('2007-10-16T13:44:39Z'), internalDocId:2313620, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"PRESS RELEASE: Exelixis Diabetic Nephropathy Drug Trial Fails Goal"}}, {uri:"https://1145.am/db/2316834/wwwfiercebiotechcom_biotech_also-noted-antigenics-releases-positive-cancer-results-novabay-raises-24m-bionovo-wraps", properties:{datePublished:datetime('2007-10-26T10:59:50Z'), internalDocId:2316834, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"ALSO NOTED: Antigenics releases positive cancer results, NovaBay raises $24M, Bionovo wraps stock offering, and much more..."}}, {uri:"https://1145.am/db/2320107/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-inc-announces-third-quarter-2007-financial-results", properties:{datePublished:datetime('2007-11-06T22:18:45Z'), internalDocId:2320107, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals, Inc. Announces Third Quarter 2007 Financial Results"}}, {uri:"https://1145.am/db/2302235/wwwfiercebiotechcom_biotech_exelixis-reports-encouraging-phase-1-data-for-xl184-at-asco", properties:{datePublished:datetime('2008-06-01T21:20:58Z'), internalDocId:2302235, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Reports Encouraging Phase 1 Data for XL184 at ASCO"}}, {uri:"https://1145.am/db/2303323/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-development-timeline-updates", properties:{datePublished:datetime('2008-06-12T15:11:31Z'), internalDocId:2303323, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces Development Timeline Updates"}}, {uri:"https://1145.am/db/2321005/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-inc-announces-final-patient-has-completed-phase-iii-clinical-trial-of", properties:{datePublished:datetime('2008-09-11T16:09:39Z'), internalDocId:2321005, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"JAZZ PHARMACEUTICALS, INC. ANNOUNCES FINAL PATIENT HAS COMPLETED PHASE III CLINICAL TRIAL OF SODIUM OXYBATE TO TREAT FIBROMYALGI"}}, {uri:"https://1145.am/db/2541506/wwwfiercepharmacom_pharma_jazz-pharma-cuts-67-jobs-on-sales-decline-king-painkiller-likely-to-get-fda-nod", properties:{datePublished:datetime('2008-11-14T13:58:09Z'), internalDocId:2541506, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharma cuts 67 jobs on sales decline, King painkiller likely to get FDA nod,"}}, {uri:"https://1145.am/db/2304748/wwwfiercebiotechcom_biotech_exelixis-cuts-10-of-workforce", properties:{datePublished:datetime('2008-11-10T13:31:45Z'), internalDocId:2304748, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis cuts 10% of workforce"}}, {uri:"https://1145.am/db/2306698/wwwfiercebiotechcom_biotech_jazz-cuts-67-jobs", properties:{datePublished:datetime('2008-11-13T15:17:47Z'), internalDocId:2306698, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz cuts 67 jobs"}}, {uri:"https://1145.am/db/2306688/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-inc-cuts-67-jobs-q3-loss-grows-to-28-8m", properties:{datePublished:datetime('2008-11-13T15:03:13Z'), internalDocId:2306688, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals, Inc. Cuts 67 Jobs, Q3 Loss Grows to $28.8M"}}, {uri:"https://1145.am/db/2315413/wwwfiercebiotechcom_biotech_jazz-and-arpida-cut-costs-jobs", properties:{datePublished:datetime('2008-12-16T15:33:40Z'), internalDocId:2315413, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz and Arpida cut costs, jobs"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/2315388/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-reduction-force-to-reflect-streamlined-operations", properties:{datePublished:datetime('2008-12-16T14:39:31Z'), internalDocId:2315388, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces Reduction in Force to Reflect Streamlined Operations"}}, {uri:"https://1145.am/db/2321447/wwwfiercebiotechcom_biotech_exelixis-chops-270-jobs-restructuring", properties:{datePublished:datetime('2010-03-09T08:25:58Z'), internalDocId:2321447, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis chops 270 jobs in restructuring"}}, {uri:"https://1145.am/db/2321449/wwwfiercebiotechcom_biotech_exelixis-announces-restructuring", properties:{datePublished:datetime('2010-03-09T12:22:20Z'), internalDocId:2321449, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces Restructuring"}}, {uri:"https://1145.am/db/2346830/wwwfiercebiotechcom_biotech_exelixis-announces-may-11-webcast-of-first-quarter-2010-financial-results-conference-call", properties:{datePublished:datetime('2010-04-28T14:19:21Z'), internalDocId:2346830, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces May 11 Webcast of First Quarter 2010 Financial Results Conference Call"}}, {uri:"https://1145.am/db/2345582/wwwfiercebiotechcom_biotech_exelixis-announces-august-5-webcast-of-second-quarter-2010-financial-results-conference", properties:{datePublished:datetime('2010-07-29T11:01:20Z'), internalDocId:2345582, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces August 5 Webcast of Second Quarter 2010 Financial Results Conference Call"}}, {uri:"https://1145.am/db/2344104/wwwfiercebiotechcom_biotech_exelixis-announces-november-4-webcast-of-third-quarter-2010-financial-results-conference", properties:{datePublished:datetime('2010-10-26T12:22:59Z'), internalDocId:2344104, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces November 4 Webcast of Third Quarter 2010 Financial Results Conference Call"}}, {uri:"https://1145.am/db/2354448/wwwfiercebiotechcom_biotech_exelixis-reports-promising-interim-data-from-patients-ovarian-cancer-treated-xl184", properties:{datePublished:datetime('2010-11-18T16:27:42Z'), internalDocId:2354448, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Reports Promising Interim Data From Patients With Ovarian Cancer Treated With XL184"}}, {uri:"https://1145.am/db/2345499/wwwfiercebiotechcom_biotech_exelixis-announces-february-22-webcast-of-its-fourth-quarter-full-year-2010-financial", properties:{datePublished:datetime('2011-02-10T11:20:27Z'), internalDocId:2345499, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces February 22 Webcast of Its Fourth Quarter & Full Year 2010 Financial Results Conference Call"}}, {uri:"https://1145.am/db/2585215/wwwfiercepharmacom_pharma_exelixis-announces-february-22-webcast-of-its-fourth-quarter-full-year-2010-financial", properties:{datePublished:datetime('2011-02-10T11:20:09Z'), internalDocId:2585215, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Exelixis Announces February 22 Webcast of Its Fourth Quarter & Full Year 2010 Financial Results Conference Call"}}, {uri:"https://1145.am/db/2350391/wwwfiercebiotechcom_biotech_exelixis-cabozantinib-phase-2-data-demonstrate-encouraging-clinical-activity-patients", properties:{datePublished:datetime('2011-06-06T18:34:46Z'), internalDocId:2350391, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis' Cabozantinib Phase 2 Data Demonstrate Encouraging Clinical Activity in Patients with Metastatic Castration-Resistant P"}}, {uri:"https://1145.am/db/2597977/wwwfiercepharmacom_pharma_viiv-pulls-app-for-new-selzentry-indication-u-s-hikma-unit-aims-for-cmo-growth-on-drug", properties:{datePublished:datetime('2011-09-21T13:07:44Z'), internalDocId:2597977, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"ViiV pulls app for new Selzentry indication, U.S. Hikma unit aims for CMO growth on drug shortages,"}}, {uri:"https://1145.am/db/2357755/wwwfiercebiotechcom_biotech_exelixis-initiates-comet-1-pivotal-trial-focused-on-overall-survival-men-advanced-prostate", properties:{datePublished:datetime('2012-05-30T14:23:56Z'), internalDocId:2357755, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Initiates COMET-1 Pivotal Trial Focused on Overall Survival in Men With Advanced Prostate Cancer"}}, {uri:"https://1145.am/db/3933752/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-enters-into-agreement-with-tersera-therapeutics-llc-for-prialt-300674775html", properties:{datePublished:datetime('2018-06-29T20:05:00Z'), internalDocId:3933752, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T03:11:45Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Enters Into Agreement with TerSera Therapeutics LLC for Prialt"}}, {uri:"https://1145.am/db/2593627/wwwfiercepharmacom_pharma_jazz-pharmaceuticals-announces-second-quarter-2012-results", properties:{datePublished:datetime('2012-08-08T01:08:08Z'), internalDocId:2593627, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals Announces Second Quarter 2012 Results"}}, {uri:"https://1145.am/db/2584928/wwwfiercepharmacom_pharma_fda-suspects-meningitis-risk-2-more-necc-drugs-novartis-clings-to-69-share-of-diovan-hct", properties:{datePublished:datetime('2012-10-16T15:42:19Z'), internalDocId:2584928, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"FDA suspects meningitis risk with 2 more NECC drugs, Novartis clings to 69% share of Diovan HCT sales,"}}, {uri:"https://1145.am/db/2343897/wwwfiercebiotechcom_biotech_medivir-splits-r-d-biomarin-launches-late-stage-study-of-pompe-drug", properties:{datePublished:datetime('2013-03-21T15:31:38Z'), internalDocId:2343897, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Medivir splits R&D, BioMarin launches late-stage study of Pompe drug,"}}, {uri:"https://1145.am/db/2351113/wwwfiercebiotechcom_biotech_exelixis-initiates-phase-3-clinical-trial-of-cabozantinib-patients-advanced-hepatocellular", properties:{datePublished:datetime('2013-09-10T14:59:49Z'), internalDocId:2351113, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Initiates Phase 3 Clinical Trial of Cabozantinib in Patients With Advanced Hepatocellular Carcinoma"}}, {uri:"https://1145.am/db/2350863/wwwfiercebiotechcom_biotech_exelixis-announces-positive-top-line-results-for-phase-3-pivotal-trial-of-cobimetinib", properties:{datePublished:datetime('2014-07-14T11:52:39Z'), internalDocId:2350863, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Announces Positive Top-Line Results for Phase 3 Pivotal Trial of Cobimetinib in Combination With Vemurafenib in Patients With BRAF V600 Mutation-Positive Advanced Melanoma"}}, {uri:"https://1145.am/db/2579294/wwwfiercepharmacom_pharma_exelixis-announces-results-from-comet-1-phase-3-pivotal-trial-of-cabozantinib-men-metastatic", properties:{datePublished:datetime('2014-09-02T15:20:42Z'), internalDocId:2579294, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Exelixis Announces Results from the COMET-1 Phase 3 Pivotal Trial of Cabozantinib in Men with Metastatic Castration-Resistant Prostate Cancer"}}, {uri:"https://1145.am/db/2561252/wwwfiercepharmacom_pharma-asia_jazz-pharmaceuticals-announces-first-patients-enrolled-phase-3-clinical-development", properties:{datePublished:datetime('2015-06-09T12:49:30Z'), internalDocId:2561252, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals Announces First Patients Enrolled in Phase 3 Clinical Development Program Evaluating JZP-110 as a Potential Treatment of Excessive Daytime Sleepiness (EDS) Associated with Narcolepsy or with Obstructive Sleep Apnea (OSA)"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/2310978/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-inc-announces-board-of-directors-and-management-changes", properties:{datePublished:datetime('2009-04-03T01:18:38Z'), internalDocId:2310978, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals, Inc. Announces Board of Directors and Management Changes"}}, {uri:"https://1145.am/db/2310994/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-announces-board-of-directors-and-management-changes-ceo-samuel-r-saks", properties:{datePublished:datetime('2009-04-03T14:42:23Z'), internalDocId:2310994, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Announces Board of Directors and Management Changes, CEO Samuel R. Saks Resigns"}}, {uri:"https://1145.am/db/2559729/wwwfiercepharmacom_vaccines_merck-ceo-frazier-touts-gardasil-sales-performance-vaccine-r-d-efforts", properties:{datePublished:datetime('2017-06-12T10:00:01Z'), internalDocId:2559729, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Merck CEO: Gardasil plus pipeline equals big things ahead for vaccines"}}, {uri:"https://1145.am/db/2301968/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-appoints-kathryn-falberg-as-chief-financial-officer", properties:{datePublished:datetime('2009-12-03T18:38:55Z'), internalDocId:2301968, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Appoints Kathryn Falberg as Chief Financial Officer"}}, {uri:"https://1145.am/db/3899582/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-full-year-and-fourth-quarter-2017-financial-results-300605221html", properties:{datePublished:datetime('2018-02-27T09:05:00Z'), internalDocId:3899582, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-19T01:11:07Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Full Year And Fourth Quarter 2017 Financial Results"}}, {uri:"https://1145.am/db/2361371/wwwfiercebiotechcom_biotech_paul-l-berns-joins-jazz-pharmaceuticals-board-of-directors", properties:{datePublished:datetime('2010-06-07T14:08:51Z'), internalDocId:2361371, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Paul L. Berns Joins Jazz Pharmaceuticals' Board of Directors"}}, {uri:"https://1145.am/db/3798778/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-third-quarter-2018-financial-results-300745059html", properties:{datePublished:datetime('2018-11-06T21:05:00Z'), internalDocId:3798778, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T19:01:44Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Third Quarter 2018 Financial Results"}}, {uri:"https://1145.am/db/3831638/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-share-repurchase-program-authorization-increase-of-400-million-300762845html", properties:{datePublished:datetime('2018-12-10T21:10:00Z'), internalDocId:3831638, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-18T20:54:42Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Share Repurchase Program Authorization Increase of $400 Million"}}, {uri:"https://1145.am/db/3619527/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-survey-highlights-prevalence-of-misinformation-and-misperception-about-narcolepsy-among-americans-300927608html", properties:{datePublished:datetime('2019-09-30T12:30:00Z'), internalDocId:3619527, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T21:56:52Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Survey Highlights Prevalence of Misinformation and Misperception About Narcolepsy Among Americans"}}, {uri:"https://1145.am/db/3615008/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-patient-enrolled-in-pivotal-phase-23-study-evaluating-jzp-458-for-the-treatment-of-acute-lymphoblastic-leukemia-or-lymphoblastic-lymphoma-300979651html", properties:{datePublished:datetime('2019-12-30T13:00:00Z'), internalDocId:3615008, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T21:42:14Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Patient Enrolled in Pivotal Phase 2/3 Study Evaluating JZP-458 for the Treatment of Acute Lymphoblastic Leukemia or Lymphoblastic Lymphoma"}}, {uri:"https://1145.am/db/3332618/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-participation-at-upcoming-investor-conferences-301011941html", properties:{datePublished:datetime('2020-02-26T21:05:00Z'), internalDocId:3332618, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-14T03:29:26Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Participation at Upcoming Investor Conferences"}}, {uri:"https://1145.am/db/88496/apnewscom_cdf53282c6c49f22fc75e493509d10f0", properties:{datePublished:datetime('2020-05-05T21:16:38Z'), internalDocId:88496, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: 1Q Earnings Snapshot"}}, {uri:"https://1145.am/db/88387/apnewscom_cca59e7fb5aadced6df5a7915caadf76", properties:{datePublished:datetime('2020-05-05T21:39:37Z'), internalDocId:88387, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: 1Q Earnings Snapshot"}}, {uri:"https://1145.am/db/2354682/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-appoints-head-of-research-and-development", properties:{datePublished:datetime('2011-09-28T16:14:32Z'), internalDocId:2354682, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Appoints Head of Research and Development"}}, {uri:"https://1145.am/db/2353684/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-appoints-head-of-research-and-development-0", properties:{datePublished:datetime('2011-09-28T12:18:55Z'), internalDocId:2353684, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals Appoints Head of Research and Development"}}, {uri:"https://1145.am/db/2575336/wwwfiercepharmacom_pharma_exelixis-appoints-executive-vice-president-and-chief-commercial-officer", properties:{datePublished:datetime('2011-10-25T11:20:24Z'), internalDocId:2575336, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Exelixis Appoints Executive Vice President and Chief Commercial Officer"}}, {uri:"https://1145.am/db/2362082/wwwfiercebiotechcom_biotech_exelixis-appoints-executive-vice-president-and-chief-commercial-officer-0", properties:{datePublished:datetime('2011-10-25T11:20:58Z'), internalDocId:2362082, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Appoints Executive Vice President and Chief Commercial Officer"}}, {uri:"https://1145.am/db/3293699/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-stops-enrollment-in-phase-3-study-evaluating-defibrotide-for-the-prevention-of-veno-occlusive-disease-301049642html", properties:{datePublished:datetime('2020-04-29T20:05:00Z'), internalDocId:3293699, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-14T02:22:50Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Stops Enrollment in Phase 3 Study Evaluating Defibrotide for the Prevention of Veno-Occlusive Disease"}}, {uri:"https://1145.am/db/2363075/wwwfiercebiotechcom_biotech_exelixis-appoints-executive-vice-president-and-chief-commercial-officer", properties:{datePublished:datetime('2011-10-25T16:29:18Z'), internalDocId:2363075, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Appoints Executive Vice President and Chief Commercial Officer"}}, {uri:"https://1145.am/db/107130/wwwreuterscom_article_us-health-coronavirus-moderna-idUSKBN23I1RG", properties:{datePublished:datetime('2020-06-11T15:26:42Z'), internalDocId:107130, deletedRedundantSameAsAt:1, sourceOrganization:"Reuters", headline:"Moderna to start final testing stage of coronavirus vaccine in July"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/2592814/wwwfiercepharmacom_pharma_jazz-pharmaceuticals-plc-announces-new-executive-appointments", properties:{datePublished:datetime('2012-01-25T15:38:24Z'), internalDocId:2592814, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals plc Announces New Executive Appointments"}}, {uri:"https://1145.am/db/3197616/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-webcast-for-xywav-investor-update-301155108html", properties:{datePublished:datetime('2020-10-19T20:15:00Z'), internalDocId:3197616, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-13T23:56:45Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Webcast for Xywav Investor Update"}}, {uri:"https://1145.am/db/3197581/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-report-2020-third-quarter-financial-results-on-november-2-2020-301155061html", properties:{datePublished:datetime('2020-10-19T20:05:00Z'), internalDocId:3197581, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-13T23:56:41Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Report 2020 Third Quarter Financial Results on November 2, 2020"}}, {uri:"https://1145.am/db/2586978/wwwfiercepharmacom_pharma_catherine-sohn-elected-to-jazz-pharmaceuticals-board-of-directors", properties:{datePublished:datetime('2012-07-31T01:07:51Z'), internalDocId:2586978, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Catherine Sohn Elected To Jazz Pharmaceuticals Board Of Directors"}}, {uri:"https://1145.am/db/3395819/wwwbusinessinsidercom_modernas-5-biggest-vaccine-programs-after-covid-19-2021-4", properties:{datePublished:datetime('2021-04-14T00:00:00Z'), internalDocId:3395819, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T15:04:47Z'), sourceOrganization:"Business Insider", headline:"Moderna is betting its mRNA technology will lead to a new wave of vaccines for diseases like HIV. Here are the top 5 it's working on beyond COVID-19."}}, {uri:"https://1145.am/db/3413145/wwwbusinessinsidercom_insiders-top-healthcare-stories-for-april-20-2021-4", properties:{datePublished:datetime('2021-04-20T00:00:00Z'), internalDocId:3413145, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T15:32:23Z'), sourceOrganization:"Business Insider", headline:"We mapped out the key changes among Walmart's healthcare leadership"}}, {uri:"https://1145.am/db/2279215/wwwfiercebiotechcom_biotech_moderna-hoping-to-prove-it-s-no-one-trick-covid-pony-posts-early-peek-at-mrna-flu-shot", properties:{datePublished:datetime('2021-12-10T13:10:00Z'), internalDocId:2279215, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Moderna, hoping to prove it's no one-trick COVID pony, posts early peek at mRNA flu shot hopeful"}}, {uri:"https://1145.am/db/4915910/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-presents-positive-interim-phase-23-results-of-rylaze-asparaginase-erwinia-chrysanthemi-recombinant-rywn-in-acute-lymphoblastic-leukemia-or-lymphoblastic-lymphoma-at-ash-2021-annual-meeting-301442693html", properties:{datePublished:datetime('2021-12-12T14:00:00Z'), internalDocId:4915910, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-05T08:26:13Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Presents Positive Interim Phase 2/3 Results of Rylaze™ (asparaginase erwinia chrysanthemi (recombinant)-rywn) in Acute Lymphoblastic Leukemia or Lymphoblastic Lymphoma at ASH 2021 Annual Meeting"}}, {uri:"https://1145.am/db/4921170/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-patient-enrolled-in-phase-2b-clinical-trial-evaluating-novel-suvecaltamide-jzp385-for-once-daily-treatment-of-adults-with-essential-tremor-301445163html", properties:{datePublished:datetime('2021-12-15T12:45:00Z'), internalDocId:4921170, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-05T08:47:47Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Patient Enrolled in Phase 2b Clinical Trial Evaluating Novel Suvecaltamide (JZP385) for Once-Daily Treatment of Adults with Essential Tremor"}}, {uri:"https://1145.am/db/4819592/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-vision-2025-to-deliver-sustainable-growth-and-enhanced-value-to-drive-transformation-to-innovative-global-biopharmaceutical-leader-301456903html", properties:{datePublished:datetime('2022-01-10T12:30:00Z'), internalDocId:4819592, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:55:27Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Vision 2025 to Deliver Sustainable Growth and Enhanced Value to Drive Transformation to Innovative, Global Biopharmaceutical Leader"}}, {uri:"https://1145.am/db/836435/wwwreuterscom_business_healthcare-pharmaceuticals_moderna-eyes-covid-booster-by-august-not-clear-yet-if-omicron-specific-needed-2022-02-16", properties:{datePublished:datetime('2022-02-16T23:06:07.178Z'), internalDocId:836435, deletedRedundantSameAsAt:1, sourceOrganization:"Reuters", headline:"Moderna eyes COVID booster by August, not clear yet if Omicron-specific needed"}}, {uri:"https://1145.am/db/844038/apnewscom_article_business-california-earnings-exelixis-inc-5923f31b2f1d607406488f8f6bb5cf72", properties:{datePublished:datetime('2022-02-17T22:09:43Z'), internalDocId:844038, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: Q4 Earnings Snapshot"}}, {uri:"https://1145.am/db/856895/apnewscom_article_business-earnings-massachusetts-moderna-inc-fd2153a3ed3455abc19ec05c40e60be6", properties:{datePublished:datetime('2022-02-24T13:54:41Z'), internalDocId:856895, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Moderna: Q4 Earnings Snapshot"}}, {uri:"https://1145.am/db/4799641/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-full-year-and-fourth-quarter-2021-financial-results-301492977html", properties:{datePublished:datetime('2022-03-01T21:05:00Z'), internalDocId:4799641, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T18:48:41Z'), sourceOrganization:"PR Newswire", headline:"JAZZ PHARMACEUTICALS ANNOUNCES FULL YEAR AND FOURTH QUARTER 2021 FINANCIAL RESULTS"}}, {uri:"https://1145.am/db/871515/apnewscom_article_business-earnings-jazz-pharmaceuticals-plc-ffbca378de70a7b91f4da31f15ab4b24", properties:{datePublished:datetime('2022-03-01T21:42:03Z'), internalDocId:871515, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: Q4 Earnings Snapshot"}}, {uri:"https://1145.am/db/4808186/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-first-quarter-2022-financial-results-and-raises-2022-financial-guidance-301539932html", properties:{datePublished:datetime('2022-05-04T20:10:00Z'), internalDocId:4808186, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-11-04T19:17:33Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces First Quarter 2022 Financial Results and Raises 2022 Financial Guidance"}}, {uri:"https://1145.am/db/1032794/apnewscom_article_business-earnings-jazz-pharmaceuticals-plc-eb5300a7c350615af1fb4447e909a7b3", properties:{datePublished:datetime('2022-05-04T20:56:37Z'), internalDocId:1032794, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: Q1 Earnings Snapshot"}}, {uri:"https://1145.am/db/1053044/apnewscom_article_business-california-earnings-exelixis-inc-e54113e535cdbda173b57b44593e778a", properties:{datePublished:datetime('2022-05-10T20:16:37Z'), internalDocId:1053044, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: Q1 Earnings Snapshot"}}, {uri:"https://1145.am/db/1465700/apnewscom_article_earnings-1491e6a0db0cac88a611b65ff0f1474a", properties:{datePublished:datetime('2022-11-01T20:47:17Z'), internalDocId:1465700, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: Q3 Earnings Snapshot"}}, {uri:"https://1145.am/db/1494821/apnewscom_article_earnings-4b89b3976016c86a26161a20d7878114", properties:{datePublished:datetime('2022-11-09T21:29:34Z'), internalDocId:1494821, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: Q3 Earnings Snapshot"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/1580058/wwwreuterscom_business_healthcare-pharmaceuticals_japan-unit-britains-jazz-pharma-starts-phase-iii-trial-cannabis-drug-2022-12-15", properties:{datePublished:datetime('2022-12-15T09:42:06.79Z'), internalDocId:1580058, deletedRedundantSameAsAt:1, sourceOrganization:"Reuters", headline:"Japan unit of Britain's Jazz Pharma starts phase III trial of cannabis drug"}}, {uri:"https://1145.am/db/2566334/wwwfiercepharmacom_pharma_jazz-wins-reimbursement-nod-nice-cannabidiol-product-epidyolex", properties:{datePublished:datetime('2023-02-01T16:52:00Z'), internalDocId:2566334, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz wins reimbursement nod from NICE for cannabinoid med Epidyolex"}}, {uri:"https://1145.am/db/1665696/apnewscom_article_automated-insights-earnings-exelixis-inc-california-business-c7bf94c8e366407bb9d62739d535e803", properties:{datePublished:datetime('2023-02-07T21:59:03Z'), internalDocId:1665696, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: Q4 Earnings Snapshot"}}, {uri:"https://1145.am/db/4711766/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-report-2022-fourth-quarter-and-full-year-financial-results-on-march-1-2023-301747989html", properties:{datePublished:datetime('2023-02-15T21:15:00Z'), internalDocId:4711766, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-29T23:06:17Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Report 2022 Fourth Quarter and Full Year Financial Results on March 1, 2023"}}, {uri:"https://1145.am/db/1688580/apnewscom_article_earnings-d3e1bce4b81a644fb4c343824f2e20bb", properties:{datePublished:datetime('2023-03-01T21:42:25Z'), internalDocId:1688580, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: Q4 Earnings Snapshot"}}, {uri:"https://1145.am/db/4755389/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-report-2023-first-quarter-financial-results-on-may-10-2023-301807430html", properties:{datePublished:datetime('2023-04-26T20:15:00Z'), internalDocId:4755389, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-30T02:00:43Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Report 2023 First Quarter Financial Results on May 10, 2023"}}, {uri:"https://1145.am/db/2000089/apnewscom_article_earnings-c178be20bd1d81e872d318321290090c", properties:{datePublished:datetime('2023-05-09T21:58:32Z'), internalDocId:2000089, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: Q1 Earnings Snapshot"}}, {uri:"https://1145.am/db/2001582/apnewscom_article_earnings-740c1de983de00851d11ecd0e966d0fd", properties:{datePublished:datetime('2023-05-10T20:36:07Z'), internalDocId:2001582, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: Q1 Earnings Snapshot"}}, {uri:"https://1145.am/db/4745705/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-host-zanidatamab-kol-investor-webcast-on-june-2-2023-301834916html", properties:{datePublished:datetime('2023-05-25T20:15:00Z'), internalDocId:4745705, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-30T01:20:47Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Host Zanidatamab KOL Investor Webcast on June 2, 2023"}}, {uri:"https://1145.am/db/4752621/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-report-2023-second-quarter-financial-results-on-august-9-2023-301886520html", properties:{datePublished:datetime('2023-07-26T20:15:00Z'), internalDocId:4752621, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-10-30T01:49:16Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Report 2023 Second Quarter Financial Results on August 9, 2023"}}, {uri:"https://1145.am/db/1254565/apnewscom_article_earnings-jazz-pharmaceuticals-plc-d5cd71c219023eac16411ba256e47d88", properties:{datePublished:datetime('2022-08-03T21:18:49Z'), internalDocId:1254565, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Jazz: Q2 Earnings Snapshot"}}, {uri:"https://1145.am/db/1270356/apnewscom_article_earnings-1932f84935025ec08c9caa753058029f", properties:{datePublished:datetime('2022-08-09T20:58:03Z'), internalDocId:1270356, deletedRedundantSameAsAt:1, sourceOrganization:"Associated Press", headline:"Exelixis: Q2 Earnings Snapshot"}}, {uri:"https://1145.am/db/4751644/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-present-at-the-42nd-annual-jp-morgan-healthcare-conference-302019557html", properties:{datePublished:datetime('2023-12-20T21:15:00Z'), internalDocId:4751644, deletedRedundantSameAsAt:1.740529705680305E9, dateRetrieved:datetime('2024-10-30T01:45:28Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Present at the 42nd Annual J.P. Morgan Healthcare Conference"}}, {uri:"https://1145.am/db/4644127/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-host-virtual-zanidatamab-rd-day-on-tuesday-march-19-2024-302087115html", properties:{datePublished:datetime('2024-03-12T20:15:00Z'), internalDocId:4644127, deletedRedundantSameAsAt:1.740529705680305E9, dateRetrieved:datetime('2024-10-29T17:54:37Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Host Virtual Zanidatamab R&D Day on Tuesday, March 19, 2024"}}, {uri:"https://1145.am/db/2353195/wwwfiercebiotechcom_financials_biotech-legend-termeer-joins-board-at-moderna", properties:{datePublished:datetime('2013-04-30T14:52:26Z'), internalDocId:2353195, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Biotech legend Termeer joins the board at Moderna"}}, {uri:"https://1145.am/db/2353227/wwwfiercebiotechcom_biotech_henri-a-termeer-joins-moderna-therapeutics-board-of-directors", properties:{datePublished:datetime('2013-05-01T18:55:08Z'), internalDocId:2353227, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Henri A. Termeer Joins Moderna Therapeutics Board of Directors"}}, {uri:"https://1145.am/db/2545689/wwwfiercepharmacom_pharma_trial-failure-jazzs-cannabis-derived-drug-blunts-goal-expand-its-use-us", properties:{datePublished:datetime('2022-06-28T14:35:00Z'), internalDocId:2545689, deletedRedundantSameAsAt:1.7406036189731367E9, sourceOrganization:"Fierce Pharma", headline:"Trial failure for Jazz's cannabis-derived drug blunts goal to expand its use to US"}}, {uri:"https://1145.am/db/4712964/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-presents-updated-phase-2a-data-at-sabcs-2023-showcasing-potential-of-zanidatamab-in-her2hr-metastatic-breast-cancer-302010341html", properties:{datePublished:datetime('2023-12-08T18:20:00Z'), internalDocId:4712964, deletedRedundantSameAsAt:1.7406036189731367E9, dateRetrieved:datetime('2024-10-29T23:12:34Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Presents Updated Phase 2a Data at SABCS 2023 Showcasing Potential of Zanidatamab in HER2+/HR+ Metastatic Breast Cancer"}}, {uri:"https://1145.am/db/3493383/wwwbusinessinsidercom_coronavirus-vaccine-funding-deal-for-moderna-from-barda-2020-4", properties:{datePublished:datetime('2020-04-16T00:00:00Z'), internalDocId:3493383, deletedRedundantSameAsAt:1, dateRetrieved:datetime('2024-01-17T17:44:13Z'), sourceOrganization:"Business Insider", headline:"Moderna's potential coronavirus vaccine just got a huge funding boost from the US government"}}, {uri:"https://1145.am/db/4691380/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-second-quarter-2024-financial-results-and-updates-2024-financial-guidance-302211459html", properties:{datePublished:datetime('2024-07-31T20:05:00Z'), internalDocId:4691380, deletedRedundantSameAsAt:1.7407685593746521E9, dateRetrieved:datetime('2024-10-29T20:18:48Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Second Quarter 2024 Financial Results and Updates 2024 Financial Guidance"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{uri:"https://1145.am/db/4691359/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-second-quarter-2024-financial-results-and-updates-2024-financial-guidance-302211410html", properties:{datePublished:datetime('2024-07-31T20:05:00Z'), internalDocId:4691359, deletedRedundantSameAsAt:1.7407685593746521E9, dateRetrieved:datetime('2024-10-29T20:18:42Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Second Quarter 2024 Financial Results and Updates 2024 Financial Guidance"}}, {uri:"https://1145.am/db/4667444/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-provides-update-on-cannabidiol-oral-solution-phase-3-trial-in-japan-in-treatment-resistant-epilepsies-302228873html", properties:{datePublished:datetime('2024-08-22T20:05:00Z'), internalDocId:4667444, deletedRedundantSameAsAt:1.7408307770675957E9, dateRetrieved:datetime('2024-10-29T19:04:38Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Provides Update on Cannabidiol Oral Solution Phase 3 Trial in Japan in Treatment-Resistant Epilepsies"}}, {uri:"https://1145.am/db/4941758/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-third-quarter-2024-financial-results-302297903html", properties:{datePublished:datetime('2024-11-06T21:05:00Z'), internalDocId:4941758, deletedRedundantSameAsAt:1.7410094788502004E9, dateRetrieved:datetime('2024-11-06T21:53:19Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Third Quarter 2024 Financial Results"}}, {uri:"https://1145.am/db/4941753/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-third-quarter-2024-financial-results-302297961html", properties:{datePublished:datetime('2024-11-06T21:05:00Z'), internalDocId:4941753, deletedRedundantSameAsAt:1.7410094788502004E9, dateRetrieved:datetime('2024-11-06T21:53:19Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Third Quarter 2024 Financial Results"}}, {uri:"https://1145.am/db/4988653/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-participate-in-citis-2024-global-healthcare-conference-302310546html", properties:{datePublished:datetime('2024-11-19T21:05:00Z'), internalDocId:4988653, deletedRedundantSameAsAt:1.7410094788502004E9, dateRetrieved:datetime('2024-11-19T21:13:19Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Participate in Citi's 2024 Global Healthcare Conference"}}, {uri:"https://1145.am/db/159841/wwwreuterscom_article_us-health-coronavirus-finance-breakingvi-idUSKBN28C342", properties:{datePublished:datetime('2020-12-02T16:41:34Z'), internalDocId:159841, deletedRedundantSameAsAt:1, sourceOrganization:"Reuters", headline:"Corona Capital: Tui bonds, Perfume comeback"}}, {uri:"https://1145.am/db/5273410/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-announces-full-year-and-fourth-quarter-2024-financial-results-and-provides-2025-financial-guidance-302385001html", properties:{datePublished:datetime('2025-02-25T21:05:00Z'), internalDocId:5273410, deletedRedundantSameAsAt:1.741096690458789E9, dateRetrieved:datetime('2025-02-27T14:38:01Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals Announces Full Year and Fourth Quarter 2024 Financial Results and Provides 2025 Financial Guidance"}}, {uri:"https://1145.am/db/5297853/wwwglobenewswirecom_news-release_2025_03_05_3037278_25619_en_Jazz-Pharmaceuticals-to-Acquire-Chimerix-Further-Diversifying-Oncology-Portfoliohtml", properties:{datePublished:datetime('2025-03-05T12:00:00Z'), internalDocId:5297853, deletedRedundantSameAsAt:1.741249911350312E9, dateRetrieved:datetime('2025-03-06T06:35:47Z'), sourceOrganization:"GlobeNewswire", headline:"Jazz Pharmaceuticals to Acquire Chimerix, Further Diversifying Oncology Portfolio"}}, {uri:"https://1145.am/db/5301824/wwwprnewswirecom_news-releases_jazz-pharmaceuticals-to-acquire-chimerix-further-diversifying-oncology-portfolio-302393029html", properties:{datePublished:datetime('2025-03-05T12:00:00Z'), internalDocId:5301824, deletedRedundantSameAsAt:1.7413416403251095E9, dateRetrieved:datetime('2025-03-06T20:25:02Z'), sourceOrganization:"PR Newswire", headline:"Jazz Pharmaceuticals to Acquire Chimerix, Further Diversifying Oncology Portfolio"}}, {uri:"https://1145.am/db/221697/wwwreuterscom_article_us-gw-pharma-m-a-jazz-pharms-idUSKBN2A31RM", properties:{datePublished:datetime('2021-02-03T19:44:03Z'), internalDocId:221697, deletedRedundantSameAsAt:1, sourceOrganization:"Reuters", headline:"Jazz Pharma to buy GW Pharma for $7.2 bln, adding cannabis-based drug to portfolio"}}, {uri:"https://1145.am/db/2316643/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-inc-announces-expansion-of-senior-debt", properties:{datePublished:datetime('2008-03-18T11:19:33Z'), internalDocId:2316643, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals, Inc. Announces Expansion of Senior Debt"}}, {uri:"https://1145.am/db/2214553/wwwfiercehealthcarecom_healthcare_exelixis-announces-june-5-webcast-conference-call-regarding-its-150-million-funding", properties:{datePublished:datetime('2008-06-05T11:22:53Z'), internalDocId:2214553, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Healthcare", headline:"Exelixis Announces June 5 Webcast of Conference Call Regarding Its $150 Million Funding Commitment from Deerfield Management"}}, {uri:"https://1145.am/db/2351047/wwwfiercebiotechcom_financials_exelixis-appoints-executive-vice-president-and-general-counsel", properties:{datePublished:datetime('2014-02-14T12:54:44Z'), internalDocId:2351047, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Exelixis Appoints Executive Vice President and General Counsel"}}, {uri:"https://1145.am/db/2354140/wwwfiercebiotechcom_biotech_jazz-pharmaceuticals-plc-jazz-announces-management-change", properties:{datePublished:datetime('2014-02-28T13:39:07Z'), internalDocId:2354140, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Biotech", headline:"Jazz Pharmaceuticals plc (JAZZ) Announces Management Change"}}, {uri:"https://1145.am/db/2593496/wwwfiercepharmacom_corporate_jazz-pharmaceuticals-names-russell-j-cox-to-position-of-chief-operating-officer", properties:{datePublished:datetime('2014-05-20T15:12:22Z'), internalDocId:2593496, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals Names Russell J. Cox To Position Of Chief Operating Officer"}}, {uri:"https://1145.am/db/2594219/wwwfiercepharmacom_pharma_jazz-pharmaceuticals-plc-jazz-names-russell-j-cox-to-position-of-chief-operating-officer", properties:{datePublished:datetime('2014-05-23T11:48:05Z'), internalDocId:2594219, deletedRedundantSameAsAt:1, sourceOrganization:"Fierce Pharma", headline:"Jazz Pharmaceuticals plc (JAZZ) Names Russell J. Cox To Position Of Chief Operating Officer"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/2635167"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/4192205"}, properties:{weight:2}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/4464368"}, properties:{weight:2}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/2964574"}, properties:{weight:74}}, {start: {uri:"https://1145.am/db/88496/Exelixis_Inc"}, end: {uri:"https://1145.am/db/geonames_location/5322737"}, properties:{weight:7}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/1645457"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/3993763"}, properties:{weight:2}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/5128581"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/5380748"}, properties:{weight:10}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/2963597"}, properties:{weight:4}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/3042142"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc"}, end: {uri:"https://1145.am/db/geonames_location/2643743"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/88496/Exelixis_Inc"}, end: {uri:"https://1145.am/db/geonames_location/5397765"}, properties:{weight:34}}, {start: {uri:"https://1145.am/db/88496/Exelixis_Inc"}, end: {uri:"https://1145.am/db/geonames_location/4887398"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/88496/Exelixis_Inc"}, end: {uri:"https://1145.am/db/geonames_location/5391959"}, properties:{weight:5}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/4931972"}, properties:{weight:15}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/2077456"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/4930956"}, properties:{weight:3}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/6251999"}, properties:{weight:2}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/5391959"}, properties:{weight:1}}] AS row
            MATCH (start:Resource{uri: row.start.uri})
            MATCH (end:Resource{uri: row.end.uri})
            CREATE (start)-[r:basedInHighGeoNamesLocation]->(end) SET r += row.properties;
            UNWIND [{start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/6254926"}, properties:{weight:2}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/6252001"}, properties:{weight:2}}, {start: {uri:"https://1145.am/db/90949/Moderna"}, end: {uri:"https://1145.am/db/geonames_location/4158928"}, properties:{weight:1}}] AS row
            MATCH (start:Resource{uri: row.start.uri})
            MATCH (end:Resource{uri: row.end.uri})
            CREATE (start)-[r:basedInHighGeoNamesLocation]->(end) SET r += row.properties;""".split(";")
        for query in queries:
            if query is None or query.strip() == "":
                continue
            db.cypher_query(query)
        R = RDFPostProcessor()
        R.run_all_in_order()
        refresh_geo_data()

    def test_does_not_find_org_with_low_relative_weight(self):
        query = """MATCH (o: Resource&Organization)-[b:basedInHighGeoNamesLocation]-(x:GeoNamesLocation) where x.admin1Code = 'CA' and x.countryCode = 'US' return o.uri, b.weight"""
        res,_ = db.cypher_query(query)
        as_set = set( [tuple(x) for x in res])
        # DB has Moderna in US-CA with weight of 1
        assert as_set == {('https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc', 10), ('https://1145.am/db/88496/Exelixis_Inc', 5), 
                            ('https://1145.am/db/88496/Exelixis_Inc', 34), ('https://1145.am/db/88496/Exelixis_Inc', 7), 
                            ('https://1145.am/db/90949/Moderna', 1) }
        ca_uris = orgs_by_industry_and_or_geo(None, "US-CA")
        assert set(ca_uris) == {'https://1145.am/db/88496/Exelixis_Inc', 'https://1145.am/db/88387/Jazz_Pharmaceuticals_Plc'}


class TestActivityHelpers(TestCase):

    def test_does_not_include_merged_orgs(self):
        '''
        based on 
        call apoc.export.cypher.query('match (o: Resource&Organization)-[t:productActivity]-(n: Resource {uri:"https://1145.am/db/5304143/Launch-Tabasco_Brand_Salsa_Picante"})-[d:documentSource]-(a: Article), (a)-[:url]-(u) return *',null,{format: "plain", stream:true})
        '''
        queries = """CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS  FOR (node:Resource) REQUIRE (node.uri) IS UNIQUE;
            UNWIND [{uri:"https://1145.am/db/5304143/Launch-Tabasco_Brand_Salsa_Picante", properties:{documentDate:datetime('2025-03-06T20:16:00Z'), deletedRedundantSameAsAt:1.7413416403251095E9, when:[localdatetime('2025-03-06T00:00:00')], whenRaw:["March 6, 2025"], productName:["TABASCO® Brand Salsa Picante"], whereHighRaw:["AVERY ISLAND, La."], whereHighClean:["Avery Island"], internalDocId:5304143, documentExtract:"AVERY ISLAND, La., March 6, 2025 /PRNewswire/ - McIlhenny Company, the makers of TABASCO® Brand Pepper Sauce, brings bold new flavor to the foodservice industry with the launch of TABASCO® Brand Salsa Picante. As the brand's first-ever Mexican-style hot sauce, TABASCO® Salsa Picante delivers a rich, thick texture, a vibrant spice blend, and a subtle kick of heat. Crafted with over 155 years of pepper expertise, this new sauce meets the high standards foodservice operators and their guests have come to expect from TABASCO® Brand. Tabasco Brand Salsa Picante packaging. Available in an easy-to-squeeze 16.2-oz.", foundName:["launch"], name:["launch"], status:["completed"]}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:ProductActivity;
            UNWIND [{uri:"https://www.prnewswire.com/news-releases/tabasco-brand-launches-new-mexican-style-hot-sauce-for-foodservice-302395079.html", properties:{deletedRedundantSameAsAt:1.7413416403251095E9}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties;
            UNWIND [{uri:"https://1145.am/db/2707859/McIlhenny_Company", properties:{internalDocId:2707859, deletedRedundantSameAsAt:1, foundName:["McIlhenny Company"], name:["McIlhenny Company"], basedInLowRaw:["Louisiana"]}}, {uri:"https://1145.am/db/5304143/McIlhenny_Company", properties:{basedInHighClean:["Avery Island"], internalDocId:5304143, deletedRedundantSameAsAt:1.7413416403251095E9, foundName:["McIlhenny Company"], name:["McIlhenny Company"], internalMergedSameAsHighToUri:"https://1145.am/db/2707859/McIlhenny_Company", basedInHighRaw:["AVERY ISLAND, La."]}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Organization;
            UNWIND [{uri:"https://1145.am/db/5304143/wwwprnewswirecom_news-releases_tabasco-brand-launches-new-mexican-style-hot-sauce-for-foodservice-302395079html", properties:{datePublished:datetime('2025-03-06T20:16:00Z'), internalDocId:5304143, deletedRedundantSameAsAt:1.7413416403251095E9, dateRetrieved:datetime('2025-03-06T22:09:18Z'), sourceOrganization:"PR Newswire", headline:"TABASCO® BRAND LAUNCHES NEW MEXICAN-STYLE HOT SAUCE FOR FOODSERVICE"}}] AS row
            CREATE (n:Resource{uri: row.uri}) SET n += row.properties SET n:Article;
            UNWIND [{start: {uri:"https://1145.am/db/5304143/McIlhenny_Company"}, end: {uri:"https://1145.am/db/5304143/Launch-Tabasco_Brand_Salsa_Picante"}, properties:{weight:1}}, {start: {uri:"https://1145.am/db/2707859/McIlhenny_Company"}, end: {uri:"https://1145.am/db/5304143/Launch-Tabasco_Brand_Salsa_Picante"}, properties:{weight:1}}] AS row
            MATCH (start:Resource{uri: row.start.uri})
            MATCH (end:Resource{uri: row.end.uri})
            CREATE (start)-[r:productActivity]->(end) SET r += row.properties;
            UNWIND [{start: {uri:"https://1145.am/db/5304143/Launch-Tabasco_Brand_Salsa_Picante"}, end: {uri:"https://1145.am/db/5304143/wwwprnewswirecom_news-releases_tabasco-brand-launches-new-mexican-style-hot-sauce-for-foodservice-302395079html"}, properties:{documentExtract:"AVERY ISLAND, La., March 6, 2025 /PRNewswire/ - McIlhenny Company, the makers of TABASCO® Brand Pepper Sauce, brings bold new flavor to the foodservice industry with the launch of TABASCO® Brand Salsa Picante. As the brand's first-ever Mexican-style hot sauce, TABASCO® Salsa Picante delivers a rich, thick texture, a vibrant spice blend, and a subtle kick of heat. Crafted with over 155 years of pepper expertise, this new sauce meets the high standards foodservice operators and their guests have come to expect from TABASCO® Brand. Tabasco Brand Salsa Picante packaging. Available in an easy-to-squeeze 16.2-oz.", weight:1}}] AS row
            MATCH (start:Resource{uri: row.start.uri})
            MATCH (end:Resource{uri: row.end.uri})
            CREATE (start)-[r:documentSource]->(end) SET r += row.properties;
            UNWIND [{start: {uri:"https://1145.am/db/5304143/wwwprnewswirecom_news-releases_tabasco-brand-launches-new-mexican-style-hot-sauce-for-foodservice-302395079html"}, end: {uri:"https://www.prnewswire.com/news-releases/tabasco-brand-launches-new-mexican-style-hot-sauce-for-foodservice-302395079.html"}, properties:{weight: 1}}] AS row
            MATCH (start:Resource{uri: row.start.uri})
            MATCH (end:Resource{uri: row.end.uri})
            CREATE (start)-[r:url]->(end) SET r += row.properties;
            """.split(";")
        clean_db()
        for query in queries:
            if query is None or query.strip() == "":
                continue
            db.cypher_query(query)           
        activity_uri = "https://1145.am/db/5304143/Launch-Tabasco_Brand_Salsa_Picante"
        act = Resource.get_by_uri(activity_uri)
        assert len(act.productOrganization) == 2   
        article_uri = "https://1145.am/db/5304143/wwwprnewswirecom_news-releases_tabasco-brand-launches-new-mexican-style-hot-sauce-for-foodservice-302395079html"
        pub_date = date.fromisoformat("2025-03-06")
        act_arts = [(activity_uri, article_uri, pub_date)]
        res = activity_articles_to_api_results(act_arts)
        assert len(res[0]['actors']['organization']) == 1
        val = res[0]['actors']['organization'].pop()
        assert val.uri == 'https://1145.am/db/2707859/McIlhenny_Company'


def check_org_and_counts(results, expected_counts_for_uri):
    vals = [ (x[0].uri,x[1]) for x in results]
    assert set(vals) == set(expected_counts_for_uri)
        