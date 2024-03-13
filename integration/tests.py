from django.test import SimpleTestCase, TestCase
from neomodel import db
import time
import os
from integration.models import DataImport
from integration.management.commands.import_ttl import do_import_ttl
from topics.models import Organization, Resource
from integration.neo4j_utils import apoc_del_redundant_med
from integration.merge_nodes import post_import_merging

'''
    Care these tests will delete
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)

def count_relevant_nodes():
    query = "match (n:Organization|RoleHolder|CorporateFinanceActivity|LocationActivity|Person|Site|Role|RoleActivity) return count(n);"
    val, _ = db.cypher_query(query)
    return val[0][0]

class TurtleLoadingTestCase(TestCase):

    def test_load_ttl_files(self):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        assert DataImport.latest_import() == None # Empty DB
        do_import_ttl(dirname="integration/test_dump/dump-1",force=True,do_archiving=False)
        node_count = count_relevant_nodes()
        latest_import = DataImport.objects.all()[0]
        assert latest_import.deletions == 0
        assert latest_import.creations == node_count

        node_will_be_deleted_and_reinstated = "https://1145.am/db/2834130/Reliance_Industries"
        node_will_be_deleted = "https://1145.am/db/2834288/Hindalco_Industries"

        assert Organization.nodes.get_or_none(uri=node_will_be_deleted_and_reinstated) is not None
        assert Organization.nodes.get_or_none(uri=node_will_be_deleted) is not None
        assert DataImport.latest_import() == 20231216081557

        do_import_ttl(dirname="integration/test_dump/dump-2",do_archiving=False)
        assert count_relevant_nodes() > node_count # More nodes than we started with

        assert Organization.nodes.get_or_none(uri=node_will_be_deleted_and_reinstated) is not None
        assert Organization.nodes.get_or_none(uri=node_will_be_deleted) is None
        assert DataImport.latest_import() == 20231224180800


class TurtlePostProcessingTestCase(SimpleTestCase):
    def setUp(self):
        ts = time.time()
        uri1 = f"http://{ts}/foo"
        uri2 = f"http://{ts}/bar"
        uri3 = f"http://{ts}/baz"

        db.cypher_query(f'''
            CREATE
            (a:Organization {{uri:"{uri1}",name:"foo"}}),
            (b:Organization {{uri:"{uri2}",name:"bar"}}),
            (c:Organization {{uri:"{uri3}",name:"baz"}}),
            (a)-[:sameAsMedium]->(b)-[:sameAsMedium]->(a),
            (a)-[:sameAsMedium]->(c)-[:sameAsMedium]->(a),
            (b)-[:sameAsMedium]->(c)-[:sameAsMedium]->(b)
            RETURN *
        ''')

        self.uri1 = uri1
        self.uri2 = uri2
        self.uri3 = uri3

    def test_deletes_not_needed_same_as(self):
        uri1 = self.uri1 # Just for convenience in case need to copy paste items below into shell
        uri2 = self.uri2
        uri3 = self.uri3

        counts1, _ = db.cypher_query(f'MATCH (n {{uri:"{uri1}"}})-[o:sameAsMedium]-(p) return count(o)' )
        counts2, _ = db.cypher_query(f'MATCH (n {{uri:"{uri2}"}})-[o:sameAsMedium]-(p) return count(o)' )
        counts3, _ = db.cypher_query(f'MATCH (n {{uri:"{uri3}"}})-[o:sameAsMedium]-(p) return count(o)' )

        assert counts1[0][0] == 4
        assert counts2[0][0] == 4
        assert counts3[0][0] == 4

        apoc_del_redundant_med()

        counts1_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri1}"}})-[o:sameAsMedium]-(p) return count(o)' )
        counts2_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri2}"}})-[o:sameAsMedium]-(p) return count(o)' )
        counts3_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri3}"}})-[o:sameAsMedium]-(p) return count(o)' )

        assert counts1_after[0][0] == 2
        assert counts2_after[0][0] == 2
        assert counts3_after[0][0] == 2

class MergeNodesTestCase():

    def test_merges_nodes(self):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        node_list = "abcdefghijklmn"
        nodes = ", ".join([make_node(x) for x in node_list])
        query = f"""CREATE {nodes},
            (a)-[:sameAsHigh]->(b),
            (a)<-[:sameAsHigh]-(c),
            (c)-[:sameAsMedium]->(d),
            (d)-[:sameAsMedium]->(e),
            (d)<-[:sameAsMedium]-(f),
            (g)-[:sameAsMedium]->(e),

            (h)-[:sameAsHigh]->(i),
            (i)-[:sameAsHigh]->(j),
            (i)<-[:sameAsHigh]-(k),
            (l)-[:sameAsMedium]->(j),
            (m)-[:sameAsMedium]->(i),
            (m)-[:sameAsHigh]->(n)
            """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == len(node_list)
        post_import_merging()
        count_of_high,_ = db.cypher_query("MATCH ()-[:sameAsHigh]-() RETURN COUNT(*)")
        assert count_of_high == [[0]]
        count_of_medium,_ = db.cypher_query("MATCH ()-[:sameAsMedium]->() RETURN COUNT(*)")
        assert(count_of_medium) == [[4]]
        assert len(Resource.nodes.all()) == 6


def make_node(letter):
    return f"({letter}: Resource {{uri:'https/1145.am/foo/{letter}', internalDocId: {ord(letter)}, val: 'bar_{letter}', name: '{letter}' }})"
