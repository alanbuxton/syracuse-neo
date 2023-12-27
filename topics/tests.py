from django.test import TestCase
from topics.models import *
from topics.graph_utils import graph_source_activity_target
from topics.timeline_utils import get_timeline_data
import os
import integration.management.commands.import_ttl as import_ttl
from integration.models import DataImport
from neomodel import db

'''
    Care these tests will delete
'''
env_var="DELETE_DB"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop database")
    exit(0)

class TestUtilsWithDumpData(TestCase):

    @classmethod
    def setUpTestData(cls):
        db.cypher_query("MATCH (n) DETACH DELETE n;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() == None # Empty DB
        import_ttl.Command().handle(dirname="dump",force=True)

    def test_corp_fin_graph_nodes_without_where(self):
        source_uri = "https://1145.am/db/2064074/Peak_Xv_Partners"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o)
        assert len(clean_node_data) == 20
        assert set([x['label'] for x in clean_node_data]) == set(
                ['3one4 Capital', 'AI4Bharat', 'Acquisition (TechCrunch 2023-09-21)',
                'Beenext', 'Investment (TechCrunch 2023-07-20)',
                'Joint Venture (Reuters 2023-07-07)', 'Kinesys', 'Lightspeed Venture',
                'M Venture Partners', 'Maka Motors', 'Northstar Group', 'Peak XV Partners',
                'Pocket Aces', 'Pratyush Kumar', 'Provident', 'Saregama', 'Sarvam',
                'Shinhan Venture Investment', 'Skystar Capital', 'Vivek Raghavan']
        )
        assert len(clean_edge_data) == 22
        assert set([x['label'] for x in clean_edge_data]) == set(
                ['INVESTOR', 'VENDOR', 'TARGET', 'BUYER', 'PROTAGONIST']
        )
        assert len(node_details) >= len(clean_node_data)
        assert len(edge_details) >= len(clean_edge_data)

    def test_corp_fin_graph_nodes_with_where(self):
        source_uri = "https://1145.am/db/2064074/Peak_Xv_Partners"
        o = Organization.nodes.get_or_none(uri=source_uri)
        clean_node_data, clean_edge_data, node_details, edge_details = graph_source_activity_target(o,include_where=True)
        assert len(clean_node_data) == 23
        assert set([x['label'] for x in clean_node_data]) == set(
                ['3one4 Capital', 'AI4Bharat', 'Acquisition (TechCrunch 2023-09-21)', 'Beenext',
                'Investment (TechCrunch 2023-07-20)', 'Joint Venture (Reuters 2023-07-07)',
                'Kinesys', 'Lightspeed Venture', 'M Venture Partners', 'Maka Motors',
                'Northstar Group', 'Peak XV Partners', 'Pocket Aces', 'Pratyush Kumar',
                'Provident', 'Saregama', 'Sarvam', 'Shinhan Venture Investment',
                'Skystar Capital', 'Vivek Raghavan', 'https://sws.geonames.org/1269750',
                'https://sws.geonames.org/1275004', 'https://sws.geonames.org/1643084']
        )
        assert len(clean_edge_data) == 31
        assert set([x['label'] for x in clean_edge_data]) == set(
                ['INVESTOR', 'BASED_IN', 'VENDOR', 'WHERE', 'TARGET', 'BUYER', 'PROTAGONIST']
        )
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
