from topics.graph_utils import *
from django.test import  TestCase
from neomodel import db
import time
from topics.models import *

def node_types(nodes):
    return set([x['entityType'] for x in nodes])

def edge_types(edges):
    return set([x['label'] for x in edges])

def assert_ids_in_details(node_or_edges, node_or_edge_details):
    node_ids = set([x['id'] for x in node_or_edges])
    node_details_keys = set(node_or_edge_details.keys())
    assert node_ids - node_details_keys == set(), f"Expected {node_ids} to be contained in {node_details_keys}"
    assert len(node_details_keys - node_ids) >= 0, f"Expected {node_ids} to be contained in {node_details_keys}"

class GraphUtilsTestCase(TestCase):
    def setUp(self):
        ts = time.time()
        uri1 = f"https://1145.am/db/{ts}/uri1"
        uri2 = f"https://1145.am/db/{ts}/uri2"
        uri3 = f"https://1145.am/db/{ts}/uri3"
        uri4 = f"https://1145.am/db/{ts}/uri4"
        uri5 = f"https://1145.am/db/{ts}/uri5"
        uri6 = f"https://1145.am/db/{ts}/uri6"
        uri7 = f"https://1145.am/db/{ts}/uri7"
        uri8 = f"https://1145.am/db/{ts}/uri8"
        uri9 = f"https://1145.am/db/{ts}/uri9"
        uri10 = f"https://1145.am/db/{ts}/uri10"
        uri11 = f"https://1145.am/db/{ts}/uri11"
        uri12 = f"https://1145.am/db/{ts}/uri12"
        uri13 = f"https://1145.am/db/{ts}/uri13"

        db.cypher_query(f'''
        CREATE
        (a:Organization:Resource {{uri:"{uri1}",name:"uri1"}}),
        (b:Organization:Resource {{uri:"{uri2}",name:"uri2"}}),
        (c:Organization:Resource {{uri:"{uri3}",name:"uri3"}}),
        (d:Role:Resource {{uri:"{uri4}",name:"uri4"}}),
        (e:RoleActivity:Resource {{uri:"{uri5}",name:"uri5",activityType:"roleactivity"}}),
        (f:Person:Resource {{uri:"{uri6}",name:"uri6"}}),
        (g:LocationActivity:Resource {{uri:"{uri7}",name:"uri7",activityType:"locationactivity"}}),
        (h:Site:Resource {{uri:"{uri8}",name:"uri8"}}),
        (i:CorporateFinanceActivity:Resource {{uri:"{uri9}",name:"uri9",activityType:"investment"}}),
        (j:Resource {{uri:"{uri10}", name:"uri10"}}),
        (k:Resource {{uri:"{uri11}", name:"uri11"}}),
        (l:Organization:Resource {{uri:"{uri12}", name:"uri12"}}),
        (m:LocationActivity:Resource {{uri:"{uri13}",name:"uri13",activityType:"locationactivity"}}),
        (a)-[:hasRole]->(d)<-[:role]-(e)<-[:roleActivity]-(f),
        (b)-[:investor]->(i)-[:target]->(c),
        (l)-[:locationAdded]->(g)-[:location]->(h),
        (a)-[:sameAsHigh]->(c),
        (b)-[:sameAsHigh]->(l),
        (a)-[:basedInHighGeoNameRDF]->(j),
        (i)-[:whereGeoNameRDF]->(k),
        (h)-[:nameGeoNameRDF]->(k),
        (l)-[:participant]->(i)
        RETURN *
        ''')

        self.uri1 = uri1
        self.uri2 = uri2
        self.uri3 = uri3
        self.uri4 = uri4
        self.uri5 = uri5
        self.uri6 = uri6
        self.uri7 = uri7
        self.uri8 = uri8
        self.uri9 = uri9
        self.uri10 = uri10
        self.uri11 = uri11
        self.uri12 = uri12

    def test_graph_centered_on_node_with_same_as(self):
        instance = Organization.nodes.get_or_none(uri=self.uri2)
        nodes, edges, node_details, edge_details = graph_source_activity_target(source_node=instance)
        assert len(nodes) == 6
        assert len(edges) == 7
        assert node_types(nodes) == set(['Cluster', 'CorporateFinanceActivity', 'LocationActivity', 'Organization', 'Site', 'Location'])
        assert edge_types(edges) == {'PARTICIPANT', 'WHERE', 'LOCATION', 'LOCATION_ADDED', 'INVESTOR', 'TARGET'}
        assert_ids_in_details(nodes, node_details)
        assert_ids_in_details(edges, edge_details)

    def test_graph_with_two_same_as_not_in_central_node(self):
        instance = Organization.nodes.get_or_none(uri=self.uri3)
        nodes, edges, node_details, edge_details = graph_source_activity_target(source_node=instance)
        assert len(nodes) == 9
        assert len(edges) == 9
        assert node_types(nodes) == {'Person', 'Location', 'Role', 'Cluster', 'Organization', 'CorporateFinanceActivity', 'RoleActivity'}
        assert edge_types(edges) == {'PARTICIPANT', 'WHERE', 'BASED_IN', 'SAME_AS_HIGH', 'ROLE', 'ROLE_HOLDER', 'HAS_ROLE', 'INVESTOR', 'TARGET'}
        assert_ids_in_details(nodes, node_details)
        assert_ids_in_details(edges, edge_details)
