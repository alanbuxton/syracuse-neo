from django.test import SimpleTestCase, TestCase
from neomodel import db
import time
import os
from integration.models import DataImport
from integration.management.commands._neo4j_utils import apoc_del_redundant_high_med
import integration.management.commands.import_ttl as import_ttl
from topics.models import Organization

'''
    Care these tests will delete
'''
env_var="DELETE_DB"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop database")
    exit(0)

def count_relevant_nodes():
    query = "match (n:Organization|RoleHolder|CorporateFinanceActivity|LocationActivity|Person|Site|Role|RoleActivity) return count(n);"
    val, _ = db.cypher_query(query)
    return val[0][0]

class TurtleLoadingTestCase(TestCase):

    def test_load_ttl_files(self):
        db.cypher_query("MATCH (n) DETACH DELETE n;")
        assert DataImport.latest_import() == None # Empty DB
        import_ttl.Command().handle(dirname="integration/test_dump/dump-1",force=True)
        node_count = count_relevant_nodes()
        latest_import = DataImport.objects.all()[0]
        assert latest_import.deletions == 0
        assert latest_import.creations == node_count

        node_will_be_deleted_and_reinstated = "https://1145.am/db/2834130/Reliance_Industries"
        node_will_be_deleted = "https://1145.am/db/2834288/Hindalco_Industries"

        assert Organization.nodes.get_or_none(uri=node_will_be_deleted_and_reinstated) is not None
        assert Organization.nodes.get_or_none(uri=node_will_be_deleted) is not None
        assert DataImport.latest_import() == 20231216081557

        import_ttl.Command().handle(dirname="integration/test_dump/dump-2")
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
            (a)-[:sameAsHigh]->(b)-[:sameAsHigh]->(a),
            (a)-[:sameAsHigh]->(c)-[:sameAsHigh]->(a),
            (b)-[:sameAsHigh]->(c)-[:sameAsHigh]->(b)
            RETURN *
        ''')

        self.uri1 = uri1
        self.uri2 = uri2
        self.uri3 = uri3

    def test_deletes_not_needed_same_as(self):
        uri1 = self.uri1 # Just for convenience in case need to copy paste items below into shell
        uri2 = self.uri2
        uri3 = self.uri3

        counts1, _ = db.cypher_query(f'MATCH (n {{uri:"{uri1}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts2, _ = db.cypher_query(f'MATCH (n {{uri:"{uri2}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts3, _ = db.cypher_query(f'MATCH (n {{uri:"{uri3}"}})-[o:sameAsHigh]-(p) return count(o)' )

        assert counts1[0][0] == 4
        assert counts2[0][0] == 4
        assert counts3[0][0] == 4

        apoc_del_redundant_high_med()

        counts1_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri1}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts2_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri2}"}})-[o:sameAsHigh]-(p) return count(o)' )
        counts3_after, _ = db.cypher_query(f'MATCH (n {{uri:"{uri3}"}})-[o:sameAsHigh]-(p) return count(o)' )

        assert counts1_after[0][0] == 2
        assert counts2_after[0][0] == 2
        assert counts3_after[0][0] == 2
