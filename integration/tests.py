from django.test import SimpleTestCase, TestCase
from neomodel import db
import time
import os
from integration.models import DataImport
from integration.management.commands.import_ttl import do_import_ttl
from topics.models import Organization, Resource, Person, ActivityMixin
from integration.neo4j_utils import delete_all_not_needed_resources, count_relationships
from integration.rdf_post_processor import RDFPostProcesser


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

def clean_db_and_load_files(dirname):
    db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
    DataImport.objects.all().delete()
    assert DataImport.latest_import() == None # Empty DB
    do_import_ttl(dirname=dirname,force=True,do_archiving=False,do_post_processing=False)
    delete_all_not_needed_resources() # Lots of "sameAs" entries that aren't available in any test data


class TurtleLoadingTestCase(TestCase):

    def test_loads_ttl_files(self):
        clean_db_and_load_files("integration/test_dump/dump-3")
        assert len(DataImport.objects.all()) == 2
        assert DataImport.latest_import() == 20231224180800
        assert Person.nodes.get_or_none(uri="https://1145.am/db/4071554/Sam_Altman") is not None # from first file
        assert Organization.nodes.get_or_none(uri="https://1145.am/db/4074773/Binance_Labs") is not None # Loaded from later file

    def test_reloads_ttl_files(self):
        clean_db_and_load_files("integration/test_dump/dump-1")
        node_count = count_relevant_nodes()
        assert node_count == 812
        latest_import = DataImport.objects.all()[0]
        assert latest_import.deletions == 0
        assert latest_import.creations > 0 and latest_import.creations <= node_count

        node_will_be_deleted_and_reinstated = "https://1145.am/db/4076092/Sauber_Group"
        node_will_be_deleted = "https://1145.am/db/4076564/Oldcastle_Buildingenvelope"

        do_import_ttl(dirname="integration/test_dump/dump-2",force=True,do_archiving=False,do_post_processing=False)
        delete_all_not_needed_resources()
        node_count2 = count_relevant_nodes()
        assert node_count2 == node_count - 1

class RdfPostProcessingTestCase(TestCase):

    def test_merges_by_same_as_high_entries_for_pair_of_orgs_with_role(self):
        clean_db_and_load_files("integration/test_dump/dump-1")
        target_node = Organization.nodes.get_or_none(uri="https://1145.am/db/4075266/Jde_Peets")
        source_node = Organization.nodes.get_or_none(uri="https://1145.am/db/4076088/Jde_Peets")
        assert len(target_node.hasRole.all()) == 3
        assert target_node.internalMergedSameAsHighStatus is None
        assert source_node.internalMergedSameAsHighStatus is None
        Organization.merge_node_connections(source_node,target_node)
        target_node2 = Organization.nodes.get_or_none(uri="https://1145.am/db/4075266/Jde_Peets")
        source_node2 = Organization.nodes.get_or_none(uri="https://1145.am/db/4076088/Jde_Peets")
        assert target_node2.internalMergedSameAsHighStatus == Organization.MERGED_TO
        assert source_node2.internalMergedSameAsHighStatus == Organization.MERGED_FROM
        assert target_node2.internalMergedSameAsHighToUri is None
        assert source_node2.internalMergedSameAsHighToUri == target_node.uri
        assert len(target_node.hasRole.all()) == 4

    def test_merges_by_same_as_high_entries_for_pair_of_orgs_with_corp_fin(self):
        clean_db_and_load_files("integration/test_dump/dump-3")
        source_node = Organization.nodes.get_or_none(uri="https://1145.am/db/4074317/Ageas")
        target_node = Organization.nodes.get_or_none(uri="https://1145.am/db/2867004/Ageas")
        assert len(target_node.buyer.all()) == 1
        assert len(source_node.buyer.all()) == 1
        Organization.merge_node_connections(source_node,target_node)
        source_node2 = Organization.nodes.get_or_none(uri="https://1145.am/db/4074317/Ageas")
        target_node2 = Organization.nodes.get_or_none(uri="https://1145.am/db/2867004/Ageas")
        assert target_node2.internalMergedSameAsHighStatus == Organization.MERGED_TO
        assert source_node2.internalMergedSameAsHighStatus == Organization.MERGED_FROM
        assert target_node2.internalMergedSameAsHighToUri is None
        assert source_node2.internalMergedSameAsHighToUri == target_node.uri
        assert len(target_node2.buyer.all()) == 2

    def test_merges_all_same_as_highs(self):
        clean_db_and_load_files("integration/test_dump/dump-1")
        rels = count_relationships()
        assert rels == 2514
        R = RDFPostProcesser()
        R.add_document_extract_to_relationship()
        R.merge_same_as_high_connections()
        rels = count_relationships()
        assert rels == 2548

        merged_tos,_ = db.cypher_query("""MATCH (n: Organization)
            WHERE n.internalMergedSameAsHighStatus = 'merged_to' RETURN *;""", resolve_objects=True)
        merged_uris = [x[0].uri for x in merged_tos]
        assert set(merged_uris) == {'https://1145.am/db/4074766/Openai',
                'https://1145.am/db/4075266/Jde_Peets',
                'https://1145.am/db/4074581/Openai'} # sameAsNameOnly between the two OpenAI examples

def clear_neo():
    db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
