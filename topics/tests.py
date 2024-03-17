from django.test import TestCase
from topics.models import *
from topics.model_queries import *
from topics.graph_utils import graph_source_activity_target
from topics.timeline_utils import get_timeline_data
import os
from integration.management.commands.import_ttl import do_import_ttl
from integration.models import DataImport
from neomodel import db
from datetime import date
from topics.cache_helpers import nuke_cache
from topics.serializers import *
from integration.merge_nodes import post_import_merging, delete_all_not_needed_resources

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
        post_import_merging()

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

    def test_corp_fin_graph_nodes_without_where(self):
        source_uri = "https://1145.am/db/3146396/Eqt_Ventures"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 8
        assert set([x['label'] for x in clean_node_data]) == set(
                ['Atomico', 'Balderton Capital', 'EQT Ventures', 'Idinvest',
                'Investment (VentureBeat: Mar 2019)', 'Peakon',
                'Peakon raises $35 million to drive employee retention through frequent surveys', 'Sunstone']
        )
        assert len(clean_edge_data) == 8 # # TODO SAME_AS* rels are redundant as they are the ones that form part of the central cluster
        assert set([x['label'] for x in clean_edge_data]) == { 'DOCUMENT_SOURCE', 'INVESTOR', 'TARGET'}
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_graph_nodes_with_where(self):
        source_uri = "https://1145.am/db/3146396/Eqt_Ventures"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 10
        assert set([x['label'] for x in clean_node_data]) == set(
                ['Atomico', 'Balderton Capital', 'EQT Ventures', 'Idinvest', 'Investment (VentureBeat: Mar 2019)',
                'Peakon', 'Peakon raises $35 million to drive employee retention through frequent surveys', 'Sunstone',
                'https://sws.geonames.org/2623032', 'https://sws.geonames.org/2658434']
        )
        assert len(clean_edge_data) == 10
        assert set([x['label'] for x in clean_edge_data]) == {'BASED_IN', 'DOCUMENT_SOURCE', 'INVESTOR', 'TARGET'}
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

    def test_location_graph_without_where(self):
        source_uri = "https://1145.am/db/4075107/Italian_Engineering_Group" # TODO - name for this org is really MAIRE (the foundName)
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 5
        assert len(clean_edge_data) == 5
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_graph_with_where(self):
        source_uri = "https://1145.am/db/4075107/Italian_Engineering_Group"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 7
        assert len(clean_edge_data) == 7
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_timeline(self):
        source_uri = "https://1145.am/db/4075107/Italian_Engineering_Group"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 4
        assert len(items) == 1
        assert set([x['label'] for x in items]) == {'Added - added Skikda petrochemical plant - has not happened'}
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_role_without_where(self):
        source_uri = "https://1145.am/db/4072168/Royal_Bank_Of_Canada"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 6
        assert len(clean_edge_data) == 7
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_role_with_where(self):
        source_uri = "https://1145.am/db/4072168/Royal_Bank_Of_Canada"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 7
        assert len(clean_edge_data) == 9
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

    def test_stats(self):
        max_date = date.fromisoformat("2024-03-10")
        counts, recents_by_geo, recents_by_source = get_stats(max_date)
        assert set(counts) == {('Person', 126), ('Organization', 1193), ('LocationActivity', 15), ('CorporateFinanceActivity', 473), ('Article', 500), ('Role', 112), ('RoleActivity', 144)}
        assert len(recents_by_geo) == 65
        assert sorted(recents_by_geo)[:20] == [('AE', 'AE', 'United Arab Emirates', 1, 1, 1), ('AE', 'AE-01', 'United Arab Emirates - Abu Dhabi', 1, 1, 1),
            ('AR', 'AR', 'Argentina', 0, 0, 1), ('AU', 'AU', 'Australia', 1, 2, 2), ('BF', 'BF', 'Burkina Faso', 0, 2, 2), ('BM', 'BM', 'Bermuda', 0, 0, 3),
            ('BR', 'BR', 'Brazil', 0, 0, 1), ('CA', 'CA', 'Canada', 22, 33, 39), ('CA', 'CA-01', 'Canada - Alberta', 1, 1, 1),
            ('CA', 'CA-02', 'Canada - British Columbia', 5, 7, 10), ('CA', 'CA-08', 'Canada - Ontario', 7, 9, 11), ('CH', 'CH', 'Switzerland', 1, 1, 1),
            ('CN', 'CN', 'China', 1, 2, 2), ('CN', 'CN-25', 'China - Shandong Sheng', 0, 1, 1), ('DE', 'DE', 'Germany', 9, 10, 14),
            ('ES', 'ES', 'Spain', 0, 2, 3), ('FR', 'FR', 'France', 2, 3, 3), ('GB', 'GB', 'United Kingdom', 7, 10, 10), ('GR', 'GR', 'Greece', 0, 4, 4),
            ('GT', 'GT', 'Guatemala', 0, 0, 1)]
        assert sorted(recents_by_source) == [('Associated Press', 1, 4, 6), ('Business Insider', 6, 6, 6), ('Business Wire', 36, 51, 51),
            ('CNN', 1, 1, 1), ('CityAM', 4, 10, 10), ('GlobeNewswire', 61, 76, 76), ('Hotel Management', 0, 5, 5), ('Luxury Travel Advisor', 1, 3, 3),
            ('MarketWatch', 11, 14, 14), ('PR Newswire', 0, 22, 107), ('Reuters', 23, 23, 23), ('Seeking Alpha', 19, 27, 27), ('South China Morning Post', 6, 6, 6),
            ('TechCrunch', 3, 4, 4), ('The Globe and Mail', 11, 13, 13), ('VentureBeat', 3, 3, 3), ('prweb', 6, 7, 7)]


    def test_recent_activities_by_country(self):
        max_date = date.fromisoformat("2024-03-10")
        min_date = date.fromisoformat("2024-03-03")
        country_code = 'CA-02'
        matching_activity_orgs = get_activities_for_serializer_by_country_and_date_range(country_code,min_date,max_date,limit=20,include_same_as=False)
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
        industry_name = "sciences, science, scientific, biotech"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry,limit=None)
        assert len(orgs) == 3

    def test_search_by_industry_only(self):
        selected_geo_name = ""
        industry_name = "sciences, science, scientific, biotech"
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry,limit=None)
        assert len(orgs) == 5

    def test_search_by_geo_only(self):
        selected_geo_name = "United Kingdom of Great Britain and Northern Ireland"
        industry_name = ""
        selected_geo = GeoSerializer(data={"country_or_region":selected_geo_name}).get_country_or_region_id()
        industry = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        orgs = get_relevant_orgs_for_country_region_industry(selected_geo,industry,limit=None)
        assert len(orgs) == 58
