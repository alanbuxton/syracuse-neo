from django.test import SimpleTestCase, TestCase
from neomodel import db
import time
import os
from integration.models import DataImport
from integration.management.commands.import_ttl import do_import_ttl
from topics.models import Organization, Resource, Person, ActivityMixin
from integration.neo4j_utils import apoc_del_redundant_same_as
from integration.merge_nodes import (post_import_merging,
    delete_all_not_needed_resources,
    reallocate_same_as_to_already_merged_nodes,
    merge_activities_for
)

'''
    Care these tests will delete Neo4j DB
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

        apoc_del_redundant_same_as()

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
        fake_same_as = "(c: Resource {uri:'https://1145.am/foo/c'})" # e.g. new sameAs added during import that wwas already merged
        new_node_query = f"""CREATE {new_node}, {fake_same_as},
             (x)-[:sameAsHigh]->(c)"""
        db.cypher_query(new_node_query)
        assert len(Resource.nodes.all()) == node_count + 2 # fake_same_as + new node
        new_node_count = count_relevant_nodes()
        assert new_node_count == node_count + 1
        post_import_merging(with_delete_not_needed_resources=True)
        assert len(Resource.nodes.all()) == 3

    def test_does_not_attempt_to_reallocate_node_if_does_not_correspond_to_a_merged_node(self):
        clear_neo()
        node_list = "ab"
        query = f"""CREATE {make_node_list(node_list)},
                    (x: Resource {{uri:'{uri("x")}'}}), // Will be deleted
                    (a)-[:sameAsHigh]->(x),
                    (b)-[:sameAsHigh]->(x)"""
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == 3
        reallocate_same_as_to_already_merged_nodes()
        assert len(Resource.nodes.all()) == 2

    def test_reallocates_node_to_all_related_nodes(self):
        clear_neo()
        node_list= "ab"
        query = f"""CREATE {make_node_list(node_list)},
                    (a)-[:sameAsHigh]->(b)"""
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == 2
        post_import_merging()
        assert len(Resource.nodes.all()) == 1
        root_node = Resource.nodes.all()[0]
        node_list2 = "cd"
        query2 = f"""CREATE {make_node_list(node_list2)},
                    (x: Resource {{uri:'{uri("b")}'}}),
                    (c)-[:sameAsHigh]->(x),
                    (d)<-[:sameAsHigh]-(x)"""
        db.cypher_query(query2)
        assert len(Resource.nodes.all()) == 4
        reallocate_same_as_to_already_merged_nodes()
        assert len(Resource.nodes.all()) == 3
        node_c = Resource.nodes.get_or_none(uri=uri('c'))
        node_d = Resource.nodes.get_or_none(uri=uri('d'))
        assert node_c is not None
        assert node_d is not None
        assert len(node_c.sameAsHigh) == 1
        assert len(node_d.sameAsHigh) == 1
        assert node_c.sameAsHigh[0].uri == root_node.uri
        assert node_d.sameAsHigh[0].uri == root_node.uri

    def test_only_merges_nodes_with_same_labels(self):
        clear_neo()
        node_list = "abc"
        org_nodes = make_node_list(node_list)
        node_x = make_node("x","Resource:Organization:Person")
        query = f"""CREATE {org_nodes},
                    {node_x},
                    (a)-[:sameAsHigh]->(b),
                    (b)-[:sameAsHigh]->(c),
                    (c)-[:sameAsHigh]->(x),
                    (b)-[:sameAsHigh]->(x)
                """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == 4
        post_import_merging(True)
        assert len(Resource.nodes.all()) == 2

    def test_merges_nodes_with_same_labels_irrespective_of_order(self):
        clear_neo()
        node_a = make_node("a","Resource:Organization:Person")
        node_b = make_node("b","Resource:Person:Organization")
        node_c = make_node("c","Resource:Organization:Site")
        query = f"""CREATE {node_a},
                    {node_b}, {node_c},
                    (a)-[:sameAsHigh]->(b),
                    (b)-[:sameAsHigh]->(c)
                """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == 3
        post_import_merging(True)
        assert len(Resource.nodes.all()) == 2

    def test_does_not_merge_same_as_high_activities(self):
        clear_neo()
        node_a = make_node("a","Resource:Organization")
        node_b = make_node("b","Resource:Organization")
        node_c = make_node("c","Resource:Organization:CorporateFinanceActivity")
        node_d = make_node("d","Resource:Organization:CorporateFinanceActivity")
        query = f"""CREATE {node_a}, {node_b}, {node_c}, {node_d},
                    (a)-[:sameAsHigh]->(b),
                    (a)-[:sameAsHigh]->(c),
                    (a)-[:sameAsHigh]->(d),
                    (b)-[:sameAsHigh]->(c),
                    (b)-[:sameAsHigh]->(d),
                    (c)-[:sameAsHigh]->(d)
                    """
        db.cypher_query(query)
        post_import_merging(True)
        assert len(Resource.nodes.all()) == 3
        assert Resource.nodes.get_or_none(uri=uri("a")) is not None
        assert Resource.nodes.get_or_none(uri=uri("b")) is None
        assert Resource.nodes.get_or_none(uri=uri("c")) is not None
        assert Resource.nodes.get_or_none(uri=uri("d")) is not None

    def test_does_not_merge_same_as_medium_activities(self):
        clear_neo()
        fake_root1 = f"""(a: Resource:Organization:CorporateFinanceActivity
                    {{ uri:'{uri("a")}', internalDocId: {ord('a')}, merged: True }} )"""
        fake_root2 = f"""(b: Resource:Organization
                    {{ uri:'{uri("b")}', internalDocId: {ord('b')}, merged: True }} )"""
        node_m = make_node("m","Resource:Organization:CorporateFinanceActivity")
        node_n = make_node("n","Resource:Organization:CorporateFinanceActivity")
        node_o = make_node("o","Resource:Organization")
        node_p = make_node("p","Resource:Organization")
        query = f"""CREATE {node_m}, {node_n}, {fake_root1},
                    {fake_root2}, {node_o}, {node_p},
                    (a)-[:sameAsMedium]->(m),
                    (a)-[:sameAsMedium]->(n),
                    (b)-[:sameAsMedium]->(o),
                    (b)-[:sameAsMedium]->(p)
                    """
        db.cypher_query(query)
        post_import_merging(True)
        assert len(Resource.nodes.all()) == 4
        assert Resource.nodes.get_or_none(uri=uri("a")) is not None
        assert Resource.nodes.get_or_none(uri=uri("b")) is not None
        assert Resource.nodes.get_or_none(uri=uri("m")) is not None
        assert Resource.nodes.get_or_none(uri=uri("n")) is not None
        assert Resource.nodes.get_or_none(uri=uri("o")) is None
        assert Resource.nodes.get_or_none(uri=uri("p")) is None

class MergeActivitiesTestCase(SimpleTestCase):

    def test_merges_activities(self):
        clear_neo()
        things = make_node_list("abcdefghi","Resource:Thing")
        joiners = make_node_list("uvwxyz","Resource:Joiner")
        # u and v should merge, w and x should merge
        query = f"""
        CREATE {things}, {joiners},
        (a)-[:activates]->(w)-[:targets]->(b),
        (a)-[:activates]->(x)-[:targets]->(b),
        (c)-[:activates]->(y)-[:targets]->(d),
        (e)-[:activates]->(z)-[:targets]->(f),
        (z)-[:targets]->(g),
        (h)-[:activates]->(u)-[:targets]->(i),
        (h)-[:activates]->(v)-[:targets]->(i)
        """
        db.cypher_query(query)
        assert len(Resource.nodes.all()) == 15
        assert len(Joiner.nodes.all()) == 6
        merge_activities_for("-[:activates]->","Joiner","-[:targets]->")
        assert len(Joiner.nodes.all()) == 6 - 2
        n1 = Joiner.nodes.get_or_none(uri=uri("u"))
        assert set(n1.name) == set(['u', 'v'])
        assert set(n1.internalDocIdList) == set([117, 118])
        assert set(n1.internalActivityList) == set([uri("u"),uri("v")])
        n_merged = Joiner.nodes.get_or_none(uri=uri("x"))
        assert n_merged is None
        n_no_change = Joiner.nodes.get_or_none(uri=uri("z"))
        assert n_no_change.internalDocIdList is None
        assert n_no_change.internalActivityList is None


class Thing(Resource):
    pass

class Joiner(Resource, ActivityMixin):
    pass

def clear_neo():
    db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")

def make_node(letter, labels="Resource:Organization"):
    return f"({letter}: {labels} {{uri:'{uri(letter)}', internalDocId: {ord(letter)}, val: 'bar_{letter}', name: '{letter}' }})"

def uri(letter):
    return f"https://1145.am/foo/{letter}"

def make_node_list(node_list, labels="Resource:Organization"):
    nodes = ", ".join([make_node(x,labels) for x in node_list])
    return nodes
