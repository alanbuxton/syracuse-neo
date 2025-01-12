from django.test import TestCase
from collections import OrderedDict
from topics.models import *
from .stats_helpers import get_stats, date_minus
from auth_extensions.anon_user_utils import create_anon_user
from .activity_helpers import get_activities_by_country_and_date_range, activities_by_industry, activities_by_region
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

    def setUp(self):
        ts = time.time()
        self.user = get_user_model().objects.create(username=f"test-{ts}")
        self.anon, _ = create_anon_user()

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
        assert len(clean_node_data) == 13
        assert len(clean_edge_data) == 20
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
        assert set(counts) == {('ProductActivity', 10), ('RoleActivity', 12), ('Article', 202), 
                               ('Person', 12), ('CorporateFinanceActivity', 194), ('PartnershipActivity', 4), 
                               ('Role', 11), ('LocationActivity', 11), ('Organization', 421)}

        assert sorted(recents_by_geo) == [('AU', 'Australia', 1, 1, 1), ('CA', 'Canada', 3, 3, 3), ('CN', 'China', 2, 2, 2), 
                                          ('CZ', 'Czechia', 1, 1, 1), ('DK', 'Denmark', 1, 1, 1), ('EG', 'Egypt', 0, 0, 1), 
                                          ('ES', 'Spain', 1, 1, 1), ('GB', 'United Kingdom of Great Britain and Northern Ireland', 3, 3, 3), 
                                          ('IE', 'Ireland', 1, 1, 1), ('IL', 'Israel', 1, 1, 1), ('IT', 'Italy', 1, 1, 1), ('JP', 'Japan', 0, 0, 1), 
                                          ('KE', 'Kenya', 1, 1, 1), ('UG', 'Uganda', 1, 1, 1), ('US', 'United States of America', 17, 17, 38)]
        sample_acts = activities_by_region("AU",date_minus(max_date,7),max_date,counts_only=False)
        assert sample_acts == [['https://1145.am/db/4290457/Gyg-Ipo', 
                                'https://1145.am/db/4290457/wwwreuterscom_markets_deals_australian-fast-food-chain-guzman-y-gomez-seeks-raise-161-mln-june-ipo-2024-05-31_', 
                                datetime(2024, 5, 31, 5, 12, 37, 320000, tzinfo=timezone.utc)]]
        sample_acts = activities_by_region("GB",date_minus(max_date,7),max_date,counts_only=False)
        assert sample_acts == [['https://1145.am/db/4290472/Associated_British_Foods-Ipo', 'https://1145.am/db/4290472/wwwmarketwatchcom_story_ab-foods-majority-shareholder-sells-10-3-mln-shares-for-gbp262-mln-067222fe', datetime(2024, 5, 31, 6, 42, tzinfo=timezone.utc)], 
                               ['https://1145.am/db/3474027/Aquiline_Technology_Growth-Gan-Investment-Series_B', 'https://1145.am/db/3474027/wwwprnewswirecom_news-releases_gan-integrity-raises-15-million-to-accelerate-global-compliance-solution-300775390html', datetime(2024, 5, 29, 13, 15, tzinfo=timezone.utc)], 
                               ['https://1145.am/db/3473030/Sylvant-Acquisition-Rights', 'https://1145.am/db/3473030/wwwprnewswirecom_news-releases_eusa-pharma-completes-acquisition-of-global-rights-to-sylvant-siltuximab--and-presents-company-update-at-37th-jp-morgan-healthcare-conference-300775508html', datetime(2024, 5, 29, 13, 0, tzinfo=timezone.utc)]]
        assert sorted(recents_by_source) == [('Associated Press', 3, 3, 3), ('Business Insider', 2, 2, 2), ('Business Wire', 1, 1, 1), 
                                             ('CityAM', 1, 1, 4), ('Fierce Pharma', 0, 0, 3), ('GlobeNewswire', 3, 3, 3), 
                                             ('Hotel Management', 0, 0, 1), ('Live Design Online', 0, 0, 1), ('MarketWatch', 4, 4, 4), 
                                             ('PR Newswire', 20, 20, 33), ('Reuters', 1, 1, 1), ('TechCrunch', 0, 0, 1), 
                                             ('The Globe and Mail', 1, 1, 1), ('VentureBeat', 0, 0, 1)]
        assert recents_by_industry[:10] == [(696, 'Architectural And Design', 0, 0, 1), (154, 'Biomanufacturing Technologies', 0, 0, 3), 
                                            (26, 'Biopharmaceutical And Biotech Industry', 1, 1, 6), (36, 'C-Commerce (\\', 1, 1, 1), (12, 'Cannabis And Hemp', 1, 1, 1), 
                                            (236, 'Chemical And Technology', 0, 0, 1), (74, 'Chip Business', 2, 2, 2), (4, 'Cloud Services', 0, 0, 1), 
                                            (165, 'Development Banks', 1, 1, 1), (134, 'Electronic Manufacturing Services And Printed Circuit Board Assembly', 1, 1, 1)]
        sample_ind = IndustryCluster.nodes.get_or_none(topicId=154)
        res = activities_by_industry(sample_ind,date_minus(max_date,90),max_date,counts_only=False)
        assert res == [['https://1145.am/db/3029576/Tesaro-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)], 
                       ['https://1145.am/db/3029576/Loxo_Oncology-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)], 
                       ['https://1145.am/db/3029576/Celgene-Acquisition', 'https://1145.am/db/3029576/wwwcityamcom_el-lilly-buys-cancer-drug-specialist-loxo-oncology-8bn_', datetime(2024, 3, 7, 18, 6, tzinfo=timezone.utc)]]

    def test_recent_activities_by_country(self):
        max_date = date.fromisoformat("2024-06-02")
        min_date = date.fromisoformat("2024-05-03")
        country_code = 'US-NY'
        matching_activity_orgs = get_activities_by_country_and_date_range(country_code,min_date,max_date,limit=20)
        assert len(matching_activity_orgs) == 6
        sorted_actors = [tuple(sorted(x['actors'].keys())) for x in matching_activity_orgs]
        assert set(sorted_actors) == {('participant', 'protagonist'), ('buyer',), ('buyer', 'target'), ('investor', 'target')}
        activity_classes = sorted([x['activity_class'] for x in matching_activity_orgs])
        assert Counter(activity_classes).most_common() == [('CorporateFinanceActivity', 6)]
        uris = sorted([x['activity_uri'] for x in matching_activity_orgs])
        assert uris == ['https://1145.am/db/3472994/Ethos_Veterinary_Health_Llc-Investment', 
                        'https://1145.am/db/3474027/Aquiline_Technology_Growth-Gan-Investment-Series_B', 
                        'https://1145.am/db/3475220/Novel_Bellevue-Investment', 
                        'https://1145.am/db/4290170/Abbvie_Inc-Acquisition', 
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
        assert set(orgs) == set(['https://1145.am/db/2543227/Celgene', 
                                 'https://1145.am/db/2364624/Parexel_International_Corporation', 
                                 'https://1145.am/db/2364647/Mersana_Therapeutics', 
                                 'https://1145.am/db/3473030/Eusa_Pharma'])

    def test_search_by_geo_only(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = ""
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is None
        orgs = orgs_by_industry_and_or_geo(industry,selected_geo)
        assert len(orgs) == 7

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

    def test_does_search_by_industry_region(self):
        client = self.client
        path = "/?industry=Hospital+Management+Service&country_or_region=United+States+of+America&earliest_date=-1"
        resp = client.get(path)
        content = str(resp.content)
        assert "https://1145.am/db/3452774/Hhaexchange" in content

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

    def test_search_industry_with_geo(self):
        client = self.client
        resp = client.get("/?industry=Biopharmaceutical+And+Biotech+Industry&country_or_region=United+States+of+America&earliest_date=-1")
        content = str(resp.content)
        assert len(re.findall(r"Celgene\s*</a>",content)) == 1
        assert len(re.findall(r"Parexel_International_Corporation\s*</a>",content)) == 1
        assert len(re.findall(r"Eusa_Pharma\s*</a>",content)) == 0

    def test_search_industry_no_geo(self):
        client = self.client
        resp = client.get("/?industry=Biopharmaceutical+And+Biotech+Industry&country_or_region=&earliest_date=-1")
        content = str(resp.content)
        assert len(re.findall(r"Celgene\s*</a>",content)) == 1
        assert len(re.findall(r"Parexel_International_Corporation\s*</a>",content)) == 1
        assert len(re.findall(r"Eusa_Pharma\s*</a>",content)) == 1

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
        assert len(re.findall("source=_all&earliest_date=-1",content)) == 13

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
        headers, ind_cluster_rows, text_row  = combined_industry_geo_results("software") 
        assert headers == [OrderedDict(
                                [('Americas', {'colspan': 5, 'classes': 'col-US col-US-CA col-US-NC col-US-NY col-US-TX'}), 
                                 ('Europe', {'colspan': 1, 'classes': 'col-DK'})]), 
                            OrderedDict(
                                [('Northern America', {'colspan': 5, 'classes': 'col-US col-US-CA col-US-NC col-US-NY col-US-TX'}), 
                                 ('Northern Europe', {'colspan': 1, 'classes': 'col-DK'})]), 
                            OrderedDict([('US', {'colspan': 5, 'classes': 'col-US col-US-CA col-US-NC col-US-NY col-US-TX'}), 
                                         ('DK', {'colspan': 1, 'classes': 'col-DK'})]), 
                            OrderedDict([('US (all)', {'colspan': 1, 'classes': 'col-US'}), 
                                         ('Northeast', {'colspan': 1, 'classes': 'col-US-NY'}), 
                                         ('South', {'colspan': 2, 'classes': 'col-US-NC col-US-TX'}), 
                                         ('West', {'colspan': 1, 'classes': 'col-US-CA'}), 
                                         ('REPEATED DK', {'colspan': 1, 'classes': 'col-DK'})]), 
                            OrderedDict([('US (all)', {'colspan': 1, 'classes': 'col-US'}), 
                                         ('Mid Atlantic', {'colspan': 1, 'classes': 'col-US-NY'}), 
                                         ('South Atlantic', {'colspan': 1, 'classes': 'col-US-NC'}), 
                                         ('West South Central', {'colspan': 1, 'classes': 'col-US-TX'}), 
                                         ('Pacific', {'colspan': 1, 'classes': 'col-US-CA'}), 
                                         ('REPEATED DK', {'colspan': 1, 'classes': 'col-DK'})]), 
                            OrderedDict([('US (all)', {'colspan': 1, 'classes': 'col-US header_final'}), 
                                         ('US-NY', {'colspan': 1, 'classes': 'col-US-NY header_final'}), 
                                         ('US-NC', {'colspan': 1, 'classes': 'col-US-NC header_final'}), 
                                         ('US-TX', {'colspan': 1, 'classes': 'col-US-TX header_final'}), 
                                         ('US-CA', {'colspan': 1, 'classes': 'col-US-CA header_final'}), 
                                         ('REPEATED DK', {'colspan': 1, 'classes': 'col-DK header_final'})])]
        assert ind_cluster_rows[:2] == [{'uri': 'https://1145.am/db/industry/109_software_tools_applications_development', 'name': 'Software-Development Tools', 'industry_id': 109, 'vals': 
                                            [{'value': 4, 'region_code': 'US'}, 
                                             {'value': 1, 'region_code': 'US-NY'}, 
                                             {'value': 0, 'region_code': 'US-NC'}, 
                                             {'value': 1, 'region_code': 'US-TX'}, 
                                             {'value': 0, 'region_code': 'US-CA'}, 
                                             {'value': 1, 'region_code': 'DK'}]}, 
                                        {'uri': 'https://1145.am/db/industry/554_software_financial_technology_solutions', 'name': 'Software And Financial Services', 'industry_id': 554, 'vals': 
                                                [{'value': 1, 'region_code': 'US'}, 
                                                {'value': 1, 'region_code': 'US-NY'}, 
                                                {'value': 0, 'region_code': 'US-NC'}, 
                                                {'value': 1, 'region_code': 'US-TX'}, 
                                                {'value': 0, 'region_code': 'US-CA'}, 
                                                {'value': 0, 'region_code': 'DK'}]}]
        assert text_row == {'uri': '', 'name': 'software', 'vals': [{'value': 5, 'region_code': 'US'}, 
                                                                    {'value': 1, 'region_code': 'US-NY'}, 
                                                                    {'value': 0, 'region_code': 'US-NC'}, 
                                                                    {'value': 1, 'region_code': 'US-TX'}, 
                                                                    {'value': 1, 'region_code': 'US-CA'}, 
                                                                    {'value': 1, 'region_code': 'DK'}]}

    def test_remove_not_needed_admin1s_from_individual_cells(self):
        all_industry_ids = [109, 554, 280, 223, 55, 182, 473]
        indiv_cells = [('109', 'US-NY'), ('554', 'US-NY'), ('554', 'US-TX'), ('280', 'US-NY'), ('223', 'US-NY'), 
                       ('55', 'US'), ('55', 'US-TX'), ('55', 'US-CA'), ('55', 'DK'), ('182', 'US-NY'),
                       ('search_str', 'US-NY'), ('search_str', 'US-CA')]
        indiv_cells = remove_not_needed_admin1s_from_individual_cells(all_industry_ids,indiv_cells)
        assert set(indiv_cells) == set([('109', 'US-NY'), ('554', 'US-NY'), ('554', 'US-TX'), ('280', 'US-NY'), 
                                        ('223', 'US-NY'), ('55', 'US'), ('55', 'DK'), ('182', 'US-NY'),
                                        ('search_str', 'US-NY'), ('search_str', 'US-CA')])

    def test_industry_geo_finder_preview(self):
        '''
                                            US (all)	US-NY	US-NC	US-TX	US-CA   DK
            Software-Development Tools	        4	    1x	    0x	    1	    0	    1
            Software And Financial Services	    1	    1x	    0x	    1x	    0	    0
            Restaurant Management	            2	    0x	    1x	    1	    0	    0
            Software And Experienced Services	1	    0x	    0x	    0	    0	    0 
            OTT Software	                    1x	    0	    0x	    1x	    0x   	0x
            Mobile Technology	                1	    0x	    0x	    1	    0	    0
            Public Safety Technology	        2x	    0x	    0x	    1x	    0x	    0x
            software (your search)	            5	    1x	    0x	    1	    1x	    1x   
        '''
        client = self.client
        payload = {'selectedIndividualCells': 
                   ['["row-109#col-US-NY","row-554#col-US-NY","row-554#col-US-TX","row-280#col-US-NY","row-223#col-US-NY","row-55#col-US","row-55#col-US-TX","row-55#col-US-CA","row-55#col-DK","row-182#col-US-NY","row-search_str#col-US-NY","row-search_str#col-US-CA","row-search_str#col-DK"]'], 
                   'selectedRows': ['["row-473"]'], 'selectedColumns': ['["col-US-NC"]'], 
                   'allIndustryIDs': ['[109, 554, 280, 223, 55, 182, 473]'], 'searchStr': ['software']}
        response = client.post("/industry_geo_finder_review",payload)
        assert response.status_code == 200
        content = str(response.content)
        assert "Public Safety Technology in all Geos" in content
        assert "Mobile Technology, OTT Software, Public Safety Technology, Restaurant Management, Software And Experienced Services, Software And Financial Services, Software-Development Tools in United States of America - North Carolina" in content
        assert "OTT Software in United States of America" in content
        assert "&quot;software&quot; in Denmark" in content
        # TODO how best to handle case where there are zero orgs in a cell but still want to track it (e.g in this case OTT Software in Denmark)
        assert len(re.findall("Denmark",content)) == 1
        assert len(re.findall("New York",content)) == 3
        assert len(re.findall("North Carolina",content)) == 1
        

class TestRegionHierarchy(TestCase):

    def setUpTestData():
        nuke_cache()

    def test_builds_region_hierarchy(self):
        countries = ["GB","CN","CA","US","ZA","AE","SG","NA","SZ"]
        admin1s = {"US":["IL","OK","IA","NY"],"CN":["11","04"]}

        country_hierarchy, country_widths, admin1_hierarchy, admin1_widths = build_region_hierarchy(countries, admin1s)

        assert country_hierarchy == {'Asia': {'Western Asia': ['AE'], 
                                                'South-eastern Asia': ['SG'], 
                                                'Eastern Asia': ['CN']}, 
                                        'Europe': {'Northern Europe': ['GB']}, 
                                        'Africa': {'Sub-Saharan Africa': 
                                                    {'Southern Africa': ['NA', 'SZ', 'ZA']}}, 
                                        'Americas': {'Northern America': ['CA', 'US']}}

        assert country_widths == {'Asia#Western Asia': 1, 'Asia#South-eastern Asia': 1, 'Asia#Eastern Asia': 1, 
                                    'Europe#Northern Europe': 1, 
                                    'Africa#Sub-Saharan Africa#Southern Africa': 3, 
                                    'Americas#Northern America': 2, 'Americas': 2, 
                                    'Africa#Sub-Saharan Africa': 3, 'Africa': 3, 'Europe': 1, 'Asia': 3}

        assert admin1_hierarchy == {'US': {'Northeast': 
                                            {'Mid Atlantic': ['NY']}, 
                                            'Midwest': 
                                            {'East North Central': ['IL'], 
                                                'West North Central': ['IA']}, 
                                            'South': {'West South Central': ['OK']}}, 
                                    'CN': ['04', '11']}

        assert admin1_widths == {'US': 
                                    {'US#Northeast#Mid Atlantic': 1, 'US#Midwest#East North Central': 1, 
                                    'US#Midwest#West North Central': 1, 'US#South#West South Central': 1, 
                                    'US#South': 1, 'US': 4, 'US#Midwest': 2, 'US#Northeast': 1}, 
                                'CN': {'CN': 2}}
            
        headers = prepare_headers(country_hierarchy, country_widths, admin1_hierarchy, admin1_widths, countries, admin1s)
        assert headers[0] == OrderedDict([('Africa', {'colspan': 3, 'classes': 'col-NA col-SZ col-ZA'}),
                                          ('Americas', {'colspan': 6, 'classes': 'col-CA col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('Asia', {'colspan': 5, 'classes': 'col-AE col-CN col-CN-04 col-CN-11 col-SG'}), 
                                          ('Europe', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[1] == OrderedDict([('Sub-Saharan Africa', {'colspan': 3, 'classes': 'col-NA col-SZ col-ZA'}), 
                                          ('Northern America', {'colspan': 6, 'classes': 'col-CA col-US col-US-IA col-US-IL col-US-NY col-US-OK'}),
                                          ('Eastern Asia', {'colspan': 3, 'classes': 'col-CN col-CN-04 col-CN-11'}), 
                                          ('South-eastern Asia', {'colspan': 1, 'classes': 'col-SG'}), 
                                          ('Western Asia', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('Northern Europe', {'colspan': 1, 'classes': 'col-GB'})])    
        assert headers[2] == OrderedDict([('Southern Africa', {'colspan': 3, 'classes': 'col-NA col-SZ col-ZA'}), 
                                          ('REPEATED Northern America', {'colspan': 6, 'classes': 'col-CA col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('REPEATED Eastern Asia', {'colspan': 3, 'classes': 'col-CN col-CN-04 col-CN-11'}), 
                                          ('REPEATED South-eastern Asia', {'colspan': 1, 'classes': 'col-SG'}), 
                                          ('REPEATED Western Asia', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('REPEATED Northern Europe', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[3] == OrderedDict([('NA', {'colspan': 1, 'classes': 'col-NA'}), ('SZ', {'colspan': 1, 'classes': 'col-SZ'}), 
                                          ('ZA', {'colspan': 1, 'classes': 'col-ZA'}), ('CA', {'colspan': 1, 'classes': 'col-CA'}), 
                                          ('US', {'colspan': 5, 'classes': 'col-US col-US-IA col-US-IL col-US-NY col-US-OK'}), 
                                          ('CN', {'colspan': 3, 'classes': 'col-CN col-CN-04 col-CN-11'}), 
                                          ('SG', {'colspan': 1, 'classes': 'col-SG'}), ('AE', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('GB', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[4] == OrderedDict([('REPEATED NA', {'colspan': 1, 'classes': 'col-NA'}), ('REPEATED SZ', {'colspan': 1, 'classes': 'col-SZ'}), 
                                          ('REPEATED ZA', {'colspan': 1, 'classes': 'col-ZA'}), ('REPEATED CA', {'colspan': 1, 'classes': 'col-CA'}), 
                                          ('US (all)', {'colspan': 1, 'classes': 'col-US'}), ('Midwest', {'colspan': 2, 'classes': 'col-US-IA col-US-IL'}), 
                                          ('Northeast', {'colspan': 1, 'classes': 'col-US-NY'}), ('South', {'colspan': 1, 'classes': 'col-US-OK'}), 
                                          ('CN (all)', {'colspan': 1, 'classes': 'col-CN'}), ('REPEATED SG', {'colspan': 1, 'classes': 'col-SG'}), 
                                          ('REPEATED AE', {'colspan': 1, 'classes': 'col-AE'}), ('REPEATED GB', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[5] == OrderedDict([('REPEATED NA', {'colspan': 1, 'classes': 'col-NA'}), ('REPEATED SZ', {'colspan': 1, 'classes': 'col-SZ'}), 
                                          ('REPEATED ZA', {'colspan': 1, 'classes': 'col-ZA'}), ('REPEATED CA', {'colspan': 1, 'classes': 'col-CA'}), 
                                          ('US (all)', {'colspan': 1, 'classes': 'col-US'}), ('East North Central', {'colspan': 1, 'classes': 'col-US-IL'}), 
                                          ('West North Central', {'colspan': 1, 'classes': 'col-US-IA'}),
                                          ('Mid Atlantic', {'colspan': 1, 'classes': 'col-US-NY'}), 
                                          ('West South Central', {'colspan': 1, 'classes': 'col-US-OK'}), ('CN (all)', {'colspan': 1, 'classes': 'col-CN'}),
                                          ('REPEATED SG', {'colspan': 1, 'classes': 'col-SG'}), ('REPEATED AE', {'colspan': 1, 'classes': 'col-AE'}), 
                                          ('REPEATED GB', {'colspan': 1, 'classes': 'col-GB'})])
        assert headers[6] == OrderedDict([('REPEATED NA', {'colspan': 1, 'classes': 'col-NA header_final'}), 
                                          ('REPEATED SZ', {'colspan': 1, 'classes': 'col-SZ header_final'}), 
                                          ('REPEATED ZA', {'colspan': 1, 'classes': 'col-ZA header_final'}), 
                                          ('REPEATED CA', {'colspan': 1, 'classes': 'col-CA header_final'}), 
                                          ('US (all)', {'colspan': 1, 'classes': 'col-US header_final'}), 
                                          ('US-IL', {'colspan': 1, 'classes': 'col-US-IL header_final'}), 
                                          ('US-IA', {'colspan': 1, 'classes': 'col-US-IA header_final'}), 
                                          ('US-NY', {'colspan': 1, 'classes': 'col-US-NY header_final'}), 
                                          ('US-OK', {'colspan': 1, 'classes': 'col-US-OK header_final'}), 
                                          ('CN (all)', {'colspan': 1, 'classes': 'col-CN header_final'}), 
                                          ('CN-04', {'colspan': 1, 'classes': 'col-CN-04 header_final'}),
                                          ('CN-11', {'colspan': 1, 'classes': 'col-CN-11 header_final'}), 
                                          ('REPEATED SG', {'colspan': 1, 'classes': 'col-SG header_final'}), 
                                          ('REPEATED AE', {'colspan': 1, 'classes': 'col-AE header_final'}), 
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

def check_org_and_counts(results, expected_counts_for_uri):
    vals = [ (x[0].uri,x[1]) for x in results]
    assert set(vals) == set(expected_counts_for_uri)
        