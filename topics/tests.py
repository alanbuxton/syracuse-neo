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
from precalculator.models import P
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
        P.nuke_all()
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
        assert set([x['label'] for x in clean_edge_data]) == {'industryClusterPrimary', 'buyer', 'basedInHighGeoNamesLocation', 'whereGeoNamesLocation', 'documentSource', 'target'}
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_timeline(self):
        source_uri = "https://1145.am/db/3558745/Jb_Hunt"
        o = Organization.self_or_ultimate_target_node(source_uri)
        groups, items, item_display_details, org_display_details = get_timeline_data(o,True)
        assert len(groups) == 4
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1

    def test_location_graph(self):
        source_uri = "https://1145.am/db/1736082/Tesla"
        o = Organization.self_or_ultimate_target_node(source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o)
        assert len(clean_node_data) == 13
        assert len(clean_edge_data) == 20
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_timeline(self):
        source_uri = "https://1145.am/db/1736082/Tesla"
        o = Organization.self_or_ultimate_target_node(source_uri)
        groups, items, item_display_details, org_display_details = get_timeline_data(o,True)
        assert len(groups) == 4
        assert len(items) == 3
        assert set([x['label'] for x in items]) == {'Added - Added Brandenburg European gigafactory - not happened at date of document', 'Added - Added Berlin European gigafactory - not happened at date of document', 'Added - Added Grüenheide European gigafactory - not happened at date of document'}
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1

    def test_role_graph(self):
        source_uri = "https://1145.am/db/1824114/Square"
        o = Organization.self_or_ultimate_target_node(source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_centered_on(o)
        assert len(clean_node_data) == 7
        assert len(clean_edge_data) == 9
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_role_timeline(self):
        source_uri = "https://1145.am/db/1824114/Square"
        o = Organization.self_or_ultimate_target_node(source_uri)
        groups, items, item_display_details, org_display_details = get_timeline_data(o,True)
        assert len(groups) == 4
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1

    def test_organization_graph_view_with_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=1")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 114
        assert "technologies" in content # from sameAsNameOnly's industry

    def test_organization_graph_view_without_same_as_name_only(self):
        client = self.client
        response = client.get("/organization/linkages/uri/1145.am/db/2166549/Discovery_Inc?combine_same_as_name_only=0")
        content = str(response.content)
        assert len(re.findall("https://1145.am/db",content)) == 50
        assert "technologies" not in content

    def test_stats(self):
        max_date = date.fromisoformat("2024-06-02")
        counts, recents_by_geo, recents_by_source = get_stats(max_date)
        assert set(counts) == {('Organization', 405), ('Article', 189), ('LocationActivity', 11), ('Person', 12), ('Role', 11), ('RoleActivity', 12), ('CorporateFinanceActivity', 194)}
        assert len(recents_by_geo) == 33
        assert sorted(recents_by_geo) == [('CA', 'CA', 'Canada', 3, 3, 3), ('CA', 'CA-08', 'Canada - Ontario', 1, 1, 1), ('CA', 'CA-10', 'Canada - Québec', 1, 1, 1), 
                                          ('CN', 'CN', 'China', 1, 1, 1), ('CZ', 'CZ', 'Czechia', 1, 1, 1), ('DK', 'DK', 'Denmark', 1, 1, 1), 
                                          ('EG', 'EG', 'Egypt', 0, 0, 1), ('ES', 'ES', 'Spain', 1, 1, 1), ('GB', 'GB', 'United Kingdom', 1, 1, 1), 
                                          ('IL', 'IL', 'Israel', 1, 1, 1), ('JP', 'JP', 'Japan', 0, 0, 1), ('KE', 'KE', 'Kenya', 1, 1, 1), 
                                          ('UG', 'UG', 'Uganda', 1, 1, 1), ('US', 'US', 'United States', 15, 15, 35), ('US', 'US-AR', 'United States - Arkansas', 1, 1, 1), 
                                          ('US', 'US-CA', 'United States - California', 1, 1, 3), ('US', 'US-DC', 'United States - District of Columbia', 1, 1, 1), 
                                          ('US', 'US-FL', 'United States - Florida', 0, 0, 2), ('US', 'US-HI', 'United States - Hawaii', 1, 1, 1), 
                                          ('US', 'US-ID', 'United States - Idaho', 1, 1, 1), ('US', 'US-IL', 'United States - Illinois', 1, 1, 3), 
                                          ('US', 'US-LA', 'United States - Louisiana', 1, 1, 3), ('US', 'US-MA', 'United States - Massachusetts', 3, 3, 4), 
                                          ('US', 'US-MD', 'United States - Maryland', 1, 1, 1), ('US', 'US-MN', 'United States - Minnesota', 1, 1, 1), 
                                          ('US', 'US-NC', 'United States - North Carolina', 0, 0, 1), ('US', 'US-NY', 'United States - New York', 4, 4, 10), 
                                          ('US', 'US-OH', 'United States - Ohio', 1, 1, 1), ('US', 'US-PA', 'United States - Pennsylvania', 0, 0, 2), 
                                          ('US', 'US-TN', 'United States - Tennessee', 1, 1, 2), ('US', 'US-TX', 'United States - Texas', 2, 2, 9), 
                                          ('US', 'US-VA', 'United States - Virginia', 1, 1, 2), ('US', 'US-WI', 'United States - Wisconsin', 1, 1, 1)]
        assert sorted(recents_by_source) == [('Business Insider', 2, 2, 2), ('Business Wire', 1, 1, 1), ('CityAM', 1, 1, 4),
            ('Fierce Pharma', 0, 0, 3), ('GlobeNewswire', 3, 3, 3), ('Hotel Management', 0, 0, 1), ('Live Design Online', 0, 0, 1),
            ('MarketWatch', 4, 4, 4), ('PR Newswire', 20, 20, 33), ('Reuters', 1, 1, 1), ('TechCrunch', 0, 0, 1),
            ('The Globe and Mail', 1, 1, 1), ('VentureBeat', 0, 0, 1)]


    def test_recent_activities_by_country(self):
        max_date = date.fromisoformat("2024-06-02")
        min_date = date.fromisoformat("2024-05-03")
        country_code = 'US-NY'
        matching_activity_orgs = get_activities_for_serializer_by_country_and_date_range(country_code,min_date,max_date,limit=20,combine_same_as_name_only=False)
        assert len(matching_activity_orgs) == 4
        sorted_participants = [tuple(sorted(x['participants'].keys())) for x in matching_activity_orgs]
        assert set(sorted_participants) == {('participant', 'protagonist'), ('investor',)}
        activity_classes = sorted([x['activity_class'] for x in matching_activity_orgs])
        assert Counter(activity_classes).most_common() == [('CorporateFinanceActivity', 4)]
        urls = sorted([x['document_url'] for x in matching_activity_orgs])
        assert urls == ['https://www.globenewswire.com/news-release/2024/06/01/2891798/0/en/CERE-NASDAQ-REMINDER-BFA-Law-Reminds-Cerevel-Shareholders-to-Inquire-About-Ongoing-Investigation-into-the-45-Merger-Offer.html',
                        'https://www.prnewswire.com/news-releases/bbh-capital-partners-completes-investment-in-ethos-veterinary-health-llc-300775214.html',
                        'https://www.prnewswire.com/news-releases/gan-integrity-raises-15-million-to-accelerate-global-compliance-solution-300775390.html',
                        'https://www.prnewswire.com/news-releases/the-praedium-group-acquires-novel-bellevue-in-nashville-tn-300775104.html']

    def test_search_by_industry_and_geo(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = "Biopharmaceutical And Biotech Industry"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None
        orgs = Organization.by_industry_and_or_geo(industry,selected_geo,limit=None)
        assert len(orgs) == 2

    def test_search_by_industry_only(self):
        selected_geo_name = ""
        industry_name = "Biopharmaceutical And Biotech Industry"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is not None # Sometimes IndustrySerializer doesn't have choices in so tests will fail
        orgs = Organization.by_industry_and_or_geo(industry,selected_geo,limit=None)
        assert len(orgs) == 6

    def test_search_by_geo_only(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = ""
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        assert industry is None
        orgs = Organization.by_industry_and_or_geo(industry,selected_geo,limit=None)
        assert len(orgs) == 7

    def test_shows_resource_page(self):
        client = self.client
        resp = client.get("/resource/1145.am/db/3544275/wwwbusinessinsidercom_hotel-zena-rbg-mural-female-women-hotel-travel-washington-dc-2019-12")
        content = str(resp.content)
        assert "https://www.businessinsider.com/hotel-zena-rbg-mural-female-women-hotel-travel-washington-dc-2019-12" in content
        assert "first female empowerment-themed hotel will open in Washington, DC with a Ruth Bader Ginsburg mural" in content
        assert "<strong>Document Url</strong>" in content
        assert "<strong>Headline</strong>" in content
        assert "<strong>Name</strong>" not in content

    def test_shows_direct_parent_child_rels(self):
        client = self.client
        resp = client.get("/organization/family-tree/uri/1145.am/db/3451381/Responsability_Investments_Ag?combine_same_as_name_only=0")
        content = str(resp.content)
        assert "REDAVIA" in content
        assert "REDOVIA" not in content

    def test_shows_parent_child_rels_via_same_as_name_only(self):
        client = self.client
        resp = client.get("/organization/family-tree/uri/1145.am/db/3451381/Responsability_Investments_Ag?combine_same_as_name_only=1")
        content = str(resp.content)
        assert "REDAVIA" in content
        assert "REDOVIA" in content

    def test_does_search_by_industry_region(self):
        client = self.client
        resp = client.get("/?industry=Hospital+Management+Service&country_or_region=United+States+-+New+York")
        content = str(resp.content)
        assert "https://1145.am/db/3452774/Hhaexchange" in content

    def test_company_search_with_combine_same_as_name_only(self):
        client = self.client
        resp = client.get("/?name=eli&combine_same_as_name_only=1")
        content = str(resp.content)
        assert "Eli Lilly" in content
        assert "https://1145.am/db/3029576/Eli_Lilly" in content
        assert "Eli Lilly and Company" not in content
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" not in content

    def test_company_search_without_combine_same_as_name_only(self):
        client = self.client
        resp = client.get("/?name=eli&combine_same_as_name_only=0")
        content = str(resp.content)
        assert "Eli Lilly" in content
        assert "https://1145.am/db/3029576/Eli_Lilly" in content
        assert "Eli Lilly and Company" in content
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" in content

    def test_search_industry_with_geo(self):
        client = self.client
        resp = client.get("/?industry=Biopharmaceutical+And+Biotech+Industry&country_or_region=United+States")
        content = str(resp.content)
        assert len(re.findall(r"Celgene\s*</a>",content)) == 1
        assert len(re.findall(r"PAREXEL International Corporation\s*</a>",content)) == 1
        assert len(re.findall(r"EUSA Pharma\s*</a>",content)) == 0

    def test_search_industry_no_geo(self):
        client = self.client
        resp = client.get("/?industry=Biopharmaceutical+And+Biotech+Industry&country_or_region=")
        content = str(resp.content)
        assert len(re.findall(r"Celgene\s*</a>",content)) == 1
        assert len(re.findall(r"PAREXEL International Corporation\s*</a>",content)) == 1
        assert len(re.findall(r"EUSA Pharma\s*</a>",content)) == 1

    def test_graph_combines_same_as_name_only_off_vs_on_based_on_target_node(self):
        client = self.client
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0")
        content0 = str(resp.content)
        activities0 = len(re.findall("Acquisition",content0))
        eli_lillies0 = len(re.findall("Eli Lilly",content0))
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=1")
        content1 = str(resp.content)
        activities1 = len(re.findall("Acquisition",content1))
        eli_lillies1 = len(re.findall("Eli Lilly",content1))
        assert activities0 == activities1
        assert eli_lillies0 > eli_lillies1 # when combined same as name only have fewer orgs

    def test_graph_combines_same_as_name_only_off_vs_on_based_on_central_node(self):
        client = self.client
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=0")
        content0 = str(resp.content)
        resp = client.get("/organization/linkages/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=1")
        content1 = str(resp.content)
        assert "https://1145.am/db/3464715/Loxo_Oncology-Acquisition" in content0
        assert "https://1145.am/db/3464715/Loxo_Oncology-Acquisition" in content1
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" not in content0
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" in content1

    def test_timeline_combines_same_as_name_only_on_off(self):
        client = self.client
        resp = client.get("/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=0")
        content0 = str(resp.content)
        resp = client.get("/organization/timeline/uri/1145.am/db/3029576/Eli_Lilly?combine_same_as_name_only=1")
        content1 = str(resp.content)
        assert "https://1145.am/db/3549221/Loxo_Oncology-Acquisition" in content0
        assert "https://1145.am/db/3549221/Loxo_Oncology-Acquisition" in content1
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" not in content0
        assert "https://1145.am/db/3448439/Loxo_Oncology-Acquisition" in content1

    def test_family_tree_same_as_name_only_on_off_parents(self):
        client = self.client
        resp = client.get("/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=0")
        content0 = str(resp.content)
        resp = client.get("/organization/family-tree/uri/1145.am/db/3029576/Loxo_Oncology?combine_same_as_name_only=1")
        content1 = str(resp.content)

        assert "https://1145.am/db/3029576/Eli_Lilly" in content0
        assert "https://1145.am/db/3029576/Eli_Lilly" in content1
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" in content0
        assert "https://1145.am/db/3448439/Eli_Lilly_And_Company" not in content1
        assert "Buyer (CityAM Mar 2024)" in content0
        assert "Buyer (CityAM Mar 2024)" in content1
        assert "Buyer (PR Newswire Jan 2019)" in content0
        assert "Buyer (PR Newswire Jan 2019)" not in content1


class TestFamilyTree(TestCase):

    def setUpTestData():
        clean_db()
        P.nuke_all() # Company name etc are stored in cache
        org_nodes = [make_node(x,y) for x,y in zip(range(100,200),"abcdefghijklmnz")]
        org_nodes = sorted(org_nodes, reverse=True)
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
        parents, other_parents = get_parent_orgs(uri,combine_same_as_name_only=False)
        assert len(parents) == 1
        uris = [x.uri for x,_,_,_,_,_ in parents]
        assert set(uris) == set(["https://1145.am/db/109/j"])
        assert len(other_parents) == 0

    def test_gets_parent_orgs_with_same_as_name_only(self):
        uri = "https://1145.am/db/111/l"
        parents, other_parents = get_parent_orgs(uri,combine_same_as_name_only=True)
        assert len(parents) == 1
        uris = [x.uri for x,_,_,_,_,_ in parents]
        assert set(uris) == set(["https://1145.am/db/109/j"])
        assert len(other_parents) == 1
        assert other_parents[0] == "https://1145.am/db/112/m"

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
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":False})
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
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":True})
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
        nodes_edges = FamilyTreeSerializer(o,context={"combine_same_as_name_only":True})
        d = nodes_edges.data
        assert len(d['nodes']) == 4

    def test_shows_nodes_in_name_order(self):
        client = self.client
        response = client.get("/organization/family-tree/uri/1145.am/db/101/b")
        content = str(response.content)
        res = re.search(r"const node_details_dict = (.+?) ;",content)
        as_dict = json.loads(res.groups(0)[0])
        names = [x['label'] for x in as_dict.values()]
        assert len(names) == 6
        bpos = names.index("Name B")
        cpos = names.index("Name C")
        fpos = names.index("Name F")
        assert bpos < cpos < fpos, f"Expected {bpos} < {cpos} < {fpos}"
