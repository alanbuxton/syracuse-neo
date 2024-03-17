from django.test import SimpleTestCase, TestCase
from neomodel import db
import time
import os
from integration.models import DataImport
from integration.management.commands.import_ttl import do_import_ttl
from topics.models import Organization, Resource, Person
from integration.neo4j_utils import apoc_del_redundant_med
from integration.merge_nodes import post_import_merging, delete_all_not_needed_resources

'''
    Care these tests will delete
'''
env_var="DELETE_NEO"
if os.environ.get(env_var) != "Y":
    print(f"Set env var {env_var}=Y to confirm you want to drop Neo4j database")
    exit(0)

def count_relevant_nodes():
    query = """MATCH (n: Resource) WHERE SIZE(LABELS(n)) > 1
                OR n.uri CONTAINS 'https://sws.geonames.org'
                RETURN COUNT(n);""" # Resource + at least one other label, or Geoname
    val, _ = db.cypher_query(query)
    return val[0][0]

def delete_all_not_needed_resources():
    query = """MATCH (n: Resource) WHERE n.uri CONTAINS 'https://1145.am/db/'
            AND SIZE(LABELS(n)) = 1
            CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;"""
    db.cypher_query(query)

class TurtleLoadingTestCase(TestCase):

    def test_loads_ttl_files(self):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() == None # Empty DB
        do_import_ttl(dirname="integration/test_dump/dump-3",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources() # Lots of "sameAs" entries that aren't available in any test data
        post_import_merging()
        assert len(DataImport.objects.all()) == 2
        assert DataImport.latest_import() == 20231224180800
        assert Person.nodes.get_or_none(uri="https://1145.am/db/4071554/Sam_Altman") is not None # from first file
        assert Organization.nodes.get_or_none(uri="https://1145.am/db/4074773/Binance_Labs") is not None # Loaded from later file

    def test_reloads_ttl_files(self):
        db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
        DataImport.objects.all().delete()
        assert DataImport.latest_import() == None # Empty DB
        do_import_ttl(dirname="integration/test_dump/dump-1",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources() # Lots of "sameAs" entries that aren't available in any test data
        post_import_merging()
        node_count = count_relevant_nodes()
        assert node_count == 802
        latest_import = DataImport.objects.all()[0]
        assert latest_import.deletions == 0
        assert latest_import.creations > 0 and latest_import.creations <= node_count

        node_will_be_deleted_and_reinstated = "https://1145.am/db/4076092/Sauber_Group"
        node_will_be_deleted = "https://1145.am/db/4076564/Oldcastle_Buildingenvelope"
        node_merged_into_another_node = "https://1145.am/db/4076088/Jde_Peets" # Shouldn't exist: gets merged into https://1145.am/db/4075266/Jde_Peets
        node_root_of_merged_nodes = "https://1145.am/db/4074766/Openai" # Won't be deleted

        n1 = Organization.nodes.get_or_none(uri=node_will_be_deleted_and_reinstated)
        assert n1 is not None

        n2 = Organization.nodes.get_or_none(uri=node_will_be_deleted)
        assert n2 is not None
        assert Organization.nodes.get_or_none(uri=node_merged_into_another_node) is None # has been merged

        n3 = Organization.get_by_merged_uri(node_merged_into_another_node)
        assert n3 is not None
        assert n3.basedInHighGeoName is None

        n4 = Organization.nodes.get_or_none(uri=node_root_of_merged_nodes)
        assert n4 is not None
        assert len(n4.industry) == 3
        assert "Transparency in AI" not in n4.industry

        assert DataImport.latest_import() == 20231216081557
        do_import_ttl(dirname="integration/test_dump/dump-2",do_archiving=False,
                    do_post_processing=False)

        n1b = Organization.nodes.get_or_none(uri=node_will_be_deleted_and_reinstated)
        assert n1b is not None
        assert n1b.element_id != n1.element_id # node was deleted and reinstated
        assert n1b.name == n1.name # not change
        assert n1b.basedInHighGeoName != n1.basedInHighGeoName
        assert n1b.basedInHighGeoName == ['Canada']
        assert n1b.industry != n1.industry
        assert n1b.industry == ['Sauberization']

        n2b = Organization.nodes.get_or_none(uri=node_will_be_deleted)
        assert n2b is None

        n3_tmp = Organization.nodes.get_or_none(uri=node_merged_into_another_node)
        assert n3_tmp.element_id != n3.element_id
        assert n3_tmp.basedInHighGeoName == ['New York City']
        n3b = Organization.get_by_merged_uri(node_merged_into_another_node)
        assert n3b == n3 # Not merged yet, but will soon be merged from n3_tmp

        n4b = Organization.nodes.get_or_none(uri=node_root_of_merged_nodes)
        assert n4b.element_id == n4.element_id # Node wasn't recreated, just added to
        assert len(n4b.industry) == len(n4.industry) + 1
        assert "Transparency in AI" in n4b.industry

        '''
        So far:
        node_will_be_deleted_and_reinstated has been recreated as expected
        node_will_be_deleted has been deleted
        '''
        post_import_merging(with_delete_not_needed_resources=True)

        n1c = Organization.nodes.get_or_none(uri=node_will_be_deleted_and_reinstated)
        assert n1c.name == n1b.name
        assert n1c.industry == n1b.industry
        assert n1c.industry != n1.industry

        n2c = Organization.nodes.get_or_none(uri=node_will_be_deleted)
        assert n2b is None

        n3_tmp_v2 = Organization.nodes.get_or_none(uri=node_merged_into_another_node)
        assert n3_tmp_v2 is None # shoud have been merged
        n3c = Organization.get_by_merged_uri(node_merged_into_another_node)
        assert n3c is not None
        assert 'New York City' in n3c.basedInHighGeoName

        n4c = Organization.nodes.get_or_none(uri=node_root_of_merged_nodes)
        assert n4c.industry == n4b.industry

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

class MergeNodesTestCase(TestCase):

    def test_merges_same_as_high(self):
        clear_neo()
        node_list = "ab"
        query = f"""
            CREATE {make_node_list(node_list)},
            (a)<-[:sameAsHigh]-(b)
        """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == len(node_list)
        post_import_merging()
        assert len(Resource.nodes.all()) == 1
        n = Resource.nodes.all()[0]
        assert n.uri == "https://1145.am/foo/a"
        assert n.name == ['a','b']
        assert n.internalDocId == 97
        assert n.internalDocIdList == [97,98]
        assert n.internalNameList == ["97_##_a","98_##_b"]
        assert n.internalSameAsHighUriList == ['https://1145.am/foo/a', 'https://1145.am/foo/b']
        assert n.internalSameAsMediumUriList is None

    def test_merges_two_same_as_high(self):
        clear_neo()
        node_list = "abcde"
        query = f"""
            CREATE {make_node_list(node_list)},
            (a)<-[:sameAsHigh]-(b),
            (b)-[:sameAsHigh]->(c),
            (d)-[:sameAsHigh]->(e)
        """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == len(node_list)
        post_import_merging()
        assert len(Resource.nodes.all()) == 2
        n0 = Resource.nodes.get_or_none(internalDocId=97)
        assert n0.uri == uri("a")
        assert n0.name == ['a','b','c']
        assert n0.internalDocId == 97
        assert set(n0.internalDocIdList) == set([97,98,99])
        assert set(n0.internalNameList) == set(["97_##_a","98_##_b","99_##_c"])
        assert set(n0.internalSameAsHighUriList) == set([uri(x) for x in "abc"])
        assert n0.internalSameAsMediumUriList is None
        n1 = Resource.nodes.get_or_none(internalDocId=100)
        assert n1.uri == uri("d")
        assert n1.name == ['d','e']
        assert n1.internalDocId == 100
        assert set(n1.internalDocIdList) == set([100, 101])
        assert set(n1.internalNameList) == set(["100_##_d","101_##_e"])
        assert set(n1.internalSameAsHighUriList) == set([uri(x) for x in "de"])
        assert n1.internalSameAsMediumUriList is None

    def test_merges_same_as_medium_if_attached_to_same_as_high(self):
        clear_neo()
        node_list = "abcde"
        query = f"""
            CREATE {make_node_list(node_list)},
            (a)<-[:sameAsHigh]-(b),
            (b)-[:sameAsMedium]->(c),
            (d)-[:sameAsMedium]->(e)
        """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == len(node_list)
        post_import_merging()
        assert len(Resource.nodes.all()) == 3 # a,b,c ; d ; e

    def test_merges_high_and_medium_nodes(self):
        clear_neo()
        node_list = "abcdefghijklmn"
        query = f"""CREATE {make_node_list(node_list)},
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
        assert count_of_medium == [[1]]
        assert len(Resource.nodes.all()) == 3

    def test_merges_incoming_node(self):
        clear_neo()
        node_list = "abcdefghijklmn"
        query = f"""CREATE {make_node_list(node_list)},
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
        node_count = count_relevant_nodes()
        new_node = make_node("x")
        fake_same_as = "(c: Resource {uri:'https://1145.am/foo/c'})"
        new_node_query = f"""CREATE {new_node}, {fake_same_as},
             (x)-[:sameAsHigh]->(c)"""
        db.cypher_query(new_node_query)
        assert len(Resource.nodes.all()) == node_count + 2 # fake_same_as + new node
        new_node_count = count_relevant_nodes()
        assert new_node_count == node_count + 1
        post_import_merging(with_delete_not_needed_resources=True)
        assert len(Resource.nodes.all()) == 3

def clear_neo():
    db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")

def make_node(letter):
    return f"({letter}: Resource:Organization {{uri:'{uri(letter)}', internalDocId: {ord(letter)}, val: 'bar_{letter}', name: '{letter}' }})"

def uri(letter):
    return f"https://1145.am/foo/{letter}"

def make_node_list(node_list):
    nodes = ", ".join([make_node(x) for x in node_list])
    return nodes
