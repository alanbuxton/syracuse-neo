from django.test import TestCase
from topics.models import *
from topics.model_queries import *
from topics.graph_utils import graph_centered_on
from topics.timeline_utils import get_timeline_data
import os
from integration.management.commands.import_ttl import do_import_ttl
from integration.models import DataImport
from neomodel import db
from datetime import date
from topics.cache_helpers import nuke_cache
from topics.serializers import *
from integration.neo4j_utils import delete_all_not_needed_resources
from integration.rdf_post_processor import RDFPostProcessor
from integration.tests import make_node, clean_db
import json
import re

'''
    Care these tests will delete neodb data
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)

class TestUtilsWithDumpData(TestCase):

    @classmethod
    def setUpTestData(cls):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() == None # Empty DB
        nuke_cache()
        do_import_ttl(dirname="dump",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources()
        r = RDFPostProcessor()
        r.run_all_in_order()

    def test_data_list_choice_field_include_great_britain_option(self):
        geo = GeoSerializer()
        field = geo.fields['country_or_region']
        assert 'United Kingdom of Great Britain and Northern Ireland' in field.choices.keys(), "Great Britain not in field choices"
        assert 'United Kingdom' in field.choices.keys()
        assert 'United Kingdom of Great Britain and Northern Ireland' in field.choices.values(), "Great Britain not in field choices"
        assert 'United Kingdom' in field.choices.values()

    def test_returns_us_country_state(self):
        geo = GeoSerializer(data={"country_or_region":"United States - Oregon"})
        assert geo.get_country_or_region_id() == 'US-OR'

    def test_returns_great_britain_gb(self):
        geo = GeoSerializer(data={"country_or_region":"United Kingdom of Great Britain and Northern Ireland"})
        assert geo.get_country_or_region_id() == 'GB'

    def test_returns_united_kingdom_gb(self):
        geo = GeoSerializer(data={"country_or_region":"United Kingdom"})
        assert geo.get_country_or_region_id() == 'GB'

    def test_corp_fin_graph_nodes(self):
        source_uri = "https://1145.am/db/3146396/Eqt_Ventures"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o)
        assert len(clean_node_data) == 8
        assert set([x['label'] for x in clean_node_data]) == set(
                ['Atomico', 'Balderton Capital', 'EQT Ventures', 'Idinvest', 'Investment (VentureBeat: Mar 2019)',
                'Peakon', 'Peakon raises $35 million to drive employee retention through frequent surveys', 'Sunstone',
                ]
        )
        assert len(clean_edge_data) == 8
        assert set([x['label'] for x in clean_edge_data]) == {'target', 'documentSource', 'investor'}
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_timeline(self):
        source_uri = "https://1145.am/db/3146396/Eqt_Ventures"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 4
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_location_graph(self):
        source_uri = "https://1145.am/db/4075107/Italian_Engineering_Group"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o)
        assert len(clean_node_data) == 5
        assert len(clean_edge_data) == 6
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_timeline(self):
        source_uri = "https://1145.am/db/4075107/Italian_Engineering_Group"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 4
        assert len(items) == 1
        assert set([x['label'] for x in items]) == {'Added - added Skikda petrochemical plant - unknown'}
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_role_graph(self):
        source_uri = "https://1145.am/db/4072168/Royal_Bank_Of_Canada"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o)
        assert len(clean_node_data) == 6
        assert len(clean_edge_data) == 8
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_role_timeline(self):
        source_uri = "https://1145.am/db/4072168/Royal_Bank_Of_Canada"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 4
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_organization_graph_view_with_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/3146906/Yamaha_Motor?include_same_as_name_only=1")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 226
        assert "Roam Robotics" in content

    def test_organization_graph_view_without_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/3146906/Yamaha_Motor?include_same_as_name_only=0")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 98
        assert  "Roam Robotics" not in content

    def test_stats(self):
        max_date = date.fromisoformat("2024-03-10")
        counts, recents_by_geo, recents_by_source = get_stats(max_date)
        assert set(counts) == {('Person', 126), ('Article', 500), ('Organization', 1202), ('RoleActivity', 144), ('CorporateFinanceActivity', 473), ('Role', 112), ('LocationActivity', 15)}
        assert len(recents_by_geo) == 65
        assert sorted(recents_by_geo)[:10] == [('AE', 'AE', 'United Arab Emirates', 1, 1, 1),
            ('AE', 'AE-01', 'United Arab Emirates - Abu Dhabi', 1, 1, 1), ('AR', 'AR', 'Argentina', 0, 0, 1),
            ('AU', 'AU', 'Australia', 1, 2, 2), ('BF', 'BF', 'Burkina Faso', 0, 2, 2), ('BM', 'BM', 'Bermuda', 0, 0, 3),
            ('BR', 'BR', 'Brazil', 0, 0, 1), ('CA', 'CA', 'Canada', 22, 33, 39), ('CA', 'CA-01', 'Canada - Alberta', 1, 1, 1),
            ('CA', 'CA-02', 'Canada - British Columbia', 5, 7, 10)]
        assert sorted(recents_by_source) == [('Associated Press', 1, 4, 6), ('Business Insider', 6, 6, 6), ('Business Wire', 36, 51, 51),
            ('CNN', 1, 1, 1), ('CityAM', 4, 10, 10), ('GlobeNewswire', 61, 76, 76), ('Hotel Management', 0, 5, 5), ('Luxury Travel Advisor', 1, 3, 3),
            ('MarketWatch', 11, 14, 14), ('PR Newswire', 0, 22, 107), ('Reuters', 23, 23, 23), ('Seeking Alpha', 19, 27, 27), ('South China Morning Post', 6, 6, 6),
            ('TechCrunch', 3, 4, 4), ('The Globe and Mail', 11, 13, 13), ('VentureBeat', 3, 3, 3), ('prweb', 6, 7, 7)]


    def test_recent_activities_by_country(self):
        max_date = date.fromisoformat("2024-03-10")
        min_date = date.fromisoformat("2024-03-03")
        country_code = 'CA-02'
        matching_activity_orgs = get_activities_for_serializer_by_country_and_date_range(country_code,min_date,max_date,limit=20,include_same_as_name_only=False)
        assert len(matching_activity_orgs) == 5
        sorted_participants = [tuple(sorted(x['participants'].keys())) for x in matching_activity_orgs]
        assert set(sorted_participants) == {(), ('vendor',), ('organization', 'person', 'role')}
        activity_classes = sorted([x['activity_class'] for x in matching_activity_orgs])
        assert Counter(activity_classes).most_common() == [('RoleActivity', 3), ('CorporateFinanceActivity', 2)]
        urls = sorted([x['document_url'] for x in matching_activity_orgs])
        assert urls == ['https://www.globenewswire.com/news-release/2024/03/08/2843337/0/en/TRILLION-ENERGY-ANNOUNCES-CFO-AND-DIRECTOR-CHANGE.html',
                        'https://www.globenewswire.com/news-release/2024/03/08/2843337/0/en/TRILLION-ENERGY-ANNOUNCES-CFO-AND-DIRECTOR-CHANGE.html',
                        'https://www.globenewswire.com/news-release/2024/03/08/2843337/0/en/TRILLION-ENERGY-ANNOUNCES-CFO-AND-DIRECTOR-CHANGE.html',
                        'https://www.globenewswire.com/news-release/2024/03/08/2843354/0/en/Mirasol-Resources-Announces-Private-Placement-Financing.html',
                        'https://www.globenewswire.com/news-release/2024/03/08/2843354/0/en/Mirasol-Resources-Announces-Private-Placement-Financing.html']


    def test_search_by_industry_and_geo(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = "sciences, science, scientific, biotech".title()
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None
        orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry,limit=None)
        assert len(orgs) == 3

    def test_search_by_industry_only(self):
        selected_geo_name = ""
        industry_name = "sciences, science, scientific, biotech".title()
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None # Sometimes IndustrySerializer doesn't have choices in so tests will fail
        orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry,limit=None)
        assert len(orgs) == 5

    def test_search_by_geo_only(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = ""
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is None
        orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry,limit=None)
        assert len(orgs) == 58

    def test_get_activities_by_geo_industry_time_range(self):
        selected_geo_name = "United States - California"
        industry_name = "investment, investments, estate, investing".title()
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None
        min_date = date.fromisoformat("2024-02-01")
        max_date = date.fromisoformat("2024-02-15")
        res = get_activities_by_date_range_industry_geo_for_api(min_date,max_date,selected_geo,industry)
        assert len(res) == 3
        min_date = date.fromisoformat("2024-02-16")
        max_date = date.fromisoformat("2024-03-31")
        res = get_activities_by_date_range_industry_geo_for_api(min_date,max_date,selected_geo,industry)
        assert len(res) == 4

    def test_shows_resource_data_with_no_docid(self):
        client = self.client
        resp = client.get("/resource/1145.am/db/wwwmarketwatchcom_story_openai-reinstates-ceo-sam-altman-to-companys-board-of-directors-after-investigation-48bdb92b")
        content = str(resp.content)
        assert "/resource/1145.am/db/wwwmarketwatchcom_story_openai-reinstates-ceo" in content
        assert "https://1145.am/db/wwwmarketwatchcom_story_openai-reinstates-ceo-sam-altman-to-companys-board-of-directors-after-investigation-48bdb92b" in content
        assert "<strong>Document Url</strong>" in content
        assert "<strong>Name</strong>" not in content

    def test_shows_resource_data_with_docid(self):
        client = self.client
        resp = client.get("/resource/1145.am/db/4076432/Sam_Altman-Starting-Board_Of_Directors")
        content = str(resp.content)
        assert "/resource/1145.am/db/4076432/Sam_Altman-Starting-Board_Of_Directors" in content
        assert "https://1145.am/db/4076432/Sam_Altman-Starting-Board_Of_Directors" in content
        assert "<strong>Document Url</strong>" not in content
        assert "<strong>Name</strong>" in content

    def test_shows_direct_parent_child_rels(self):
        client = self.client
        resp = client.get("/organization/family-tree/uri/1145.am/db/3147748/Blackrock")
        content = str(resp.content)
        assert "Rivian" in content
        assert "Blackrock to acquire the rest of SpiderRock Advisors" in content
        assert "https://venturebeat.com/entrepreneur/rivian-raises-1-3-billion-for-its-electric-pickup-truck-and-suv/" in content

    def test_shows_parent_child_rels_via_same_as_name_only(self):
        client = self.client
        resp = client.get("/organization/family-tree/uri/1145.am/db/4076145/Blackrock")
        content = str(resp.content)
        assert "Rivian" in content
        assert "Blackrock to acquire the rest of SpiderRock Advisors" in content
        assert "https://venturebeat.com/entrepreneur/rivian-raises-1-3-billion-for-its-electric-pickup-truck-and-suv/" in content

class TestFamilyTree(TestCase):

    def setUpTestData():
        clean_db()
        nuke_cache() # Company name etc are stored in cache
        org_nodes = [make_node(x,y) for x,y in zip(range(100,200),"abcdefghijklmnz")]
        act_nodes = [make_node(x,y,"CorporateFinanceActivity") for x,y in zip(range(100,200),"opqrstuvw")]
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
            (m)-[:buyer]->(v)-[:target]->(n)
        """
        db.cypher_query(query)

    def test_gets_parent_orgs_without_same_as_name_only(self):
        uri = "https://1145.am/db/111/l"
        parents, other_parents = get_parent_orgs(uri,include_same_as_name_only=False)
        assert len(parents) == 1
        uris = [x.uri for x,_,_,_,_ in parents]
        assert set(uris) == set(["https://1145.am/db/109/j"])
        assert len(other_parents) == 0

    def test_gets_parent_orgs_with_same_as_name_only(self):
        uri = "https://1145.am/db/111/l"
        parents, other_parents = get_parent_orgs(uri,include_same_as_name_only=True)
        assert len(parents) == 1
        uris = [x.uri for x,_,_,_,_ in parents]
        assert set(uris) == set(["https://1145.am/db/109/j"])
        assert len(other_parents) == 1
        assert other_parents[0] == "https://1145.am/db/112/m"

    def test_gets_child_orgs_without_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        children = get_child_orgs(uri,include_same_as_name_only=False)
        assert len(children) == 1
        child_uris = [x.uri for (x,_,_,_,_) in children]
        assert set(child_uris) == set(["https://1145.am/db/103/d"])

    def test_gets_child_orgs_with_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        children = get_child_orgs(uri)
        assert len(children) == 2
        child_uris = [x.uri for (x,_,_,_,_) in children]
        assert set(child_uris) == set(["https://1145.am/db/103/d","https://1145.am/db/107/h"])

    def test_gets_family_tree_without_same_as_name_only(self):
        uri = "https://1145.am/db/101/b"
        o = Organization.self_or_ultimate_target_node(uri)
        nodes_edges = FamilyTreeSerializer(o,context={"include_same_as_name_only":False})
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
        nodes_edges = FamilyTreeSerializer(o,context={"include_same_as_name_only":True})
        d = nodes_edges.data
        assert len(d['nodes']) == 6
        assert len(d['edges']) == 5
        edge_details = json.loads(d['edge_details'])
        assert len(edge_details) == len(d['edges'])
        node_details = json.loads(d['node_details'])
        assert len(node_details) == len(d['nodes'])
