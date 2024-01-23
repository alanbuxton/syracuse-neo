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

'''
    Care these tests will delete
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
        do_import_ttl(dirname="dump",force=True,do_archiving=False)

    def test_corp_fin_graph_nodes_without_where(self):
        source_uri = "https://1145.am/db/2064074/Peak_Xv_Partners"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 20
        assert set([x['label'] for x in clean_node_data]) == set(
                ['3one4 Capital', 'AI4Bharat', 'Acquisition (TechCrunch: Sep 2023)',
                'Beenext', 'Investment (TechCrunch: Jul 2023)',
                'Joint Venture (Reuters: Jul 2023)', 'Kinesys', 'Lightspeed Venture',
                'M Venture Partners', 'Maka Motors', 'Northstar Group', 'Peak XV Partners',
                'Pocket Aces', 'Pratyush Kumar', 'Provident', 'Saregama', 'Sarvam',
                'Shinhan Venture Investment', 'Skystar Capital', 'Vivek Raghavan']
        )
        assert len(clean_edge_data) == 24 # # TODO SAME_AS* rels are redundant as they are the ones that form part of the central cluster
        assert set([x['label'] for x in clean_edge_data]) == {'BUYER', 'VENDOR', 'TARGET', 'SAME_AS_MEDIUM',
                                            'SAME_AS_HIGH', 'INVESTOR', 'PROTAGONIST'}
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_graph_nodes_with_where(self):
        source_uri = "https://1145.am/db/2064074/Peak_Xv_Partners"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 23
        assert set([x['label'] for x in clean_node_data]) == set(
                ['3one4 Capital', 'AI4Bharat', 'Acquisition (TechCrunch: Sep 2023)', 'Beenext',
                'Investment (TechCrunch: Jul 2023)', 'Joint Venture (Reuters: Jul 2023)',
                'Kinesys', 'Lightspeed Venture', 'M Venture Partners', 'Maka Motors',
                'Northstar Group', 'Peak XV Partners', 'Pocket Aces', 'Pratyush Kumar',
                'Provident', 'Saregama', 'Sarvam', 'Shinhan Venture Investment',
                'Skystar Capital', 'Vivek Raghavan', 'https://sws.geonames.org/1269750',
                'https://sws.geonames.org/1275004', 'https://sws.geonames.org/1643084']
        )
        assert len(clean_edge_data) == 33 # TODO SAME_AS* rels are redundant as they are the ones that form part of the central cluster
        assert set([x['label'] for x in clean_edge_data]) == {'WHERE', 'BUYER', 'VENDOR', 'BASED_IN', 'TARGET',
                                            'SAME_AS_MEDIUM', 'SAME_AS_HIGH', 'INVESTOR', 'PROTAGONIST'}
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_timeline(self):
        source_uri = "https://1145.am/db/2064074/Peak_Xv_Partners"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 4
        assert len(items) == 3
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_location_graph_without_where(self):
        source_uri = "https://1145.am/db/2212366/Swedish_Orthopedic_Institute"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 5
        assert len(clean_edge_data) == 4
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_graph_with_where(self):
        source_uri = "https://1145.am/db/2212366/Swedish_Orthopedic_Institute"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 7
        assert len(clean_edge_data) == 7
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_timeline(self):
        source_uri = "https://1145.am/db/2212366/Swedish_Orthopedic_Institute"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 3
        assert len(items) == 2
        assert set([x['label'] for x in items]) == {'Added - added Seattle Orthopedic Institute - completed',
                                                    'Added - added Hill Orthopedic Institute - completed'}
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_role_without_where(self):
        source_uri = "https://1145.am/db/2136786/New_York_Mets"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 4
        assert len(clean_edge_data) == 3
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_role_without_where(self):
        source_uri = "https://1145.am/db/2136786/New_York_Mets"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 5
        assert len(clean_edge_data) == 4
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_location_timeline(self):
        source_uri = "https://1145.am/db/2136786/New_York_Mets"
        o = Organization.nodes.get_or_none(uri=source_uri)
        groups, items, item_display_details, org_display_details, errors = get_timeline_data([o])
        assert len(groups) == 4
        assert len(items) == 1
        assert len(item_display_details) >= len(items)
        assert len(org_display_details) == 1
        assert errors == set()

    def test_stats(self):
        max_date = date.fromisoformat("2023-10-10")
        counts, recents_by_geo, recents_by_source = get_stats(max_date)
        assert set(counts) == {('CorporateFinanceActivity', 363), ('Organization', 1390), ('RoleActivity', 173), ('Person', 163), ('LocationActivity', 257)}
        assert len(recents_by_geo) == 58
        assert sorted(recents_by_geo) == [('AR', 'AR', 'Argentina', 0, 0, 4), ('AU', 'AU', 'Australia', 2, 4, 7),
                ('BE', 'BE', 'Belgium', 0, 1, 4), ('BF', 'BF', 'Burkina Faso', 1, 1, 1), ('BM', 'BM', 'Bermuda', 0, 1, 1),
                ('BR', 'BR', 'Brazil', 0, 0, 2), ('CA', 'CA', 'Canada', 4, 6, 11),
                ('CA', 'CA-02', '- British Columbia', 1, 1, 3), ('CA', 'CA-08', '- Ontario', 1, 1, 2),
                ('CH', 'CH', 'Switzerland', 0, 3, 4), ('CL', 'CL', 'Chile', 0, 0, 1),
                ('CN', 'CN', 'China', 1, 4, 8), ('CN', 'CN-09', '- Henan Sheng', 0, 1, 1),
                ('CN', 'CN-22', '- Beijing Shi', 0, 0, 1), ('CN', 'CN-23', '- Shanghai Shi', 0, 2, 2),
                ('CR', 'CR', 'Costa Rica', 0, 1, 1), ('CY', 'CY', 'Cyprus', 0, 0, 1), ('CZ', 'CZ', 'Czechia', 0, 0, 1),
                ('DE', 'DE', 'Germany', 0, 3, 6), ('DK', 'DK', 'Denmark', 1, 1, 1), ('FR', 'FR', 'France', 1, 5, 10),
                ('GB', 'GB', 'United Kingdom', 1, 1, 6), ('GH', 'GH', 'Ghana', 0, 0, 1), ('HK', 'HK', 'Hong Kong', 0, 0, 1),
                ('HU', 'HU', 'Hungary', 0, 1, 1), ('ID', 'ID', 'Indonesia', 0, 0, 1), ('IE', 'IE', 'Ireland', 1, 1, 1),
                ('IL', 'IL', 'Israel', 0, 0, 1), ('IN', 'IN', 'India', 0, 3, 10), ('IS', 'IS', 'Iceland', 0, 0, 1),
                ('IT', 'IT', 'Italy', 0, 3, 9), ('JP', 'JP', 'Japan', 0, 2, 3), ('KR', 'KR', 'Korea, Republic of', 0, 1, 1),
                ('MX', 'MX', 'Mexico', 0, 1, 1), ('NL', 'NL', 'Netherlands', 0, 0, 3), ('NO', 'NO', 'Norway', 0, 0, 1),
                ('NP', 'NP', 'Nepal', 0, 0, 1), ('RU', 'RU', 'Russian Federation', 0, 0, 3), ('SA', 'SA', 'Saudi Arabia', 0, 1, 2),
                ('SE', 'SE', 'Sweden', 0, 0, 1), ('SG', 'SG', 'Singapore', 0, 3, 3), ('SN', 'SN', 'Senegal', 1, 1, 1),
                ('TW', 'TW', 'Taiwan, Province of China', 0, 0, 3), ('US', 'US', 'United States', 18, 34, 60),
                ('US', 'US-AR', '- Arkansas', 1, 1, 1), ('US', 'US-CA', '- California', 2, 6, 13),
                ('US', 'US-DC', '- District of Columbia', 0, 1, 1), ('US', 'US-FL', '- Florida', 1, 2, 3),
                ('US', 'US-LA', '- Louisiana', 7, 7, 7), ('US', 'US-MA', '- Massachusetts', 0, 0, 1),
                ('US', 'US-MO', '- Missouri', 0, 0, 1), ('US', 'US-NY', '- New York', 1, 2, 3), ('US', 'US-OH', '- Ohio', 0, 0, 1),
                ('US', 'US-TX', '- Texas', 1, 2, 4), ('US', 'US-UT', '- Utah', 2, 3, 4), ('US', 'US-WA', '- Washington', 0, 0, 1),
                ('US', 'US-WV', '- West Virginia', 0, 0, 2), ('ZA', 'ZA', 'South Africa', 1, 1, 1)]
        assert sorted(recents_by_source) == [('Associated Press', 5, 6, 6), ('Business Wire', 171, 181, 205),
                    ('PR Web', 0, 0, 1), ('Reuters', 18, 36, 79),
                    ('TechCrunch', 2, 7, 27), ('prweb', 11, 14, 16)]

    def test_recent_activities_by_country(self):
        max_date = date.fromisoformat("2023-10-10")
        min_date = date.fromisoformat("2023-10-03")
        country_code = "AU"
        matching_activity_orgs = get_activities_for_serializer_by_country_and_date_range(country_code,min_date,max_date,limit=20,include_same_as=False)
        assert len(matching_activity_orgs) == 2
        assert matching_activity_orgs[0]['participants'].get("investor") is not None
        assert matching_activity_orgs[0]['participants'].get("buyer") is not None
        assert len(matching_activity_orgs[0]['participants']) == 2
        assert len(matching_activity_orgs[1]['participants']) == 1
