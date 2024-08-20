from django.test import SimpleTestCase, TestCase
from neomodel import db
import time
import os
from datetime import datetime, timezone
from integration.models import DataImport
from integration.management.commands.import_ttl import ( do_import_ttl,
    load_deletion_file, load_file
)
from topics.models import Organization, Resource, Person, ActivityMixin
from integration.neo4j_utils import (
    delete_all_not_needed_resources, count_relationships,
    apoc_del_redundant_same_as,
)
from integration.rdf_post_processor import RDFPostProcessor
from precalculator.models import P
import logging
logger = logging.getLogger(__name__)


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

def clean_db():
    db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")
    DataImport.objects.all().delete()

def clean_db_and_load_files(dirname,do_post_processing=False):
    clean_db()
    assert DataImport.latest_import() == None # Empty DB
    do_import_ttl(dirname=dirname,force=True,do_archiving=False,do_post_processing=do_post_processing)
    delete_all_not_needed_resources() # Lots of "sameAs" entries that aren't available in any test data
    apoc_del_redundant_same_as()

class TurtleLoadingTestCase(TestCase):

    def test_loads_ttl_files(self):
        clean_db_and_load_files("integration/test_dump/dump-3")
        assert len(DataImport.objects.all()) == 2
        assert DataImport.latest_import() == 20231224180800

    def test_loads_deletion_and_recreation_step_by_step(self):
        clean_db_and_load_files("integration/test_dump/dump-1",do_post_processing=True)
        node_count = count_relevant_nodes()
        assert node_count == 1765
        latest_import = DataImport.objects.all()[0]
        assert latest_import.deletions == 0
        assert latest_import.creations > 0 and latest_import.creations <= node_count

        # 4001762 is the one that will be recreated
        assert Organization.self_or_ultimate_target_node("https://1145.am/db/4290155/Royal_Mail").uri == "https://1145.am/db/4001762/Royal_Mail"
        assert Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail").uri == "https://1145.am/db/4001762/Royal_Mail"
        assert Organization.self_or_ultimate_target_node("https://1145.am/db/2984033/Royal_Mail") is None

        # 2984033 should be the ultimate target node - next load will ensure this is the case
        do_import_ttl(dirname="integration/test_dump/dump-1.5",force=True,do_archiving=False,do_post_processing=True)
        assert Organization.self_or_ultimate_target_node("https://1145.am/db/4290155/Royal_Mail").uri == "https://1145.am/db/2984033/Royal_Mail"
        assert Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail").uri == "https://1145.am/db/2984033/Royal_Mail"
        assert Organization.self_or_ultimate_target_node("https://1145.am/db/2984033/Royal_Mail").uri == "https://1145.am/db/2984033/Royal_Mail"

        ultimate_target_node = Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail")
        assert ultimate_target_node.uri == "https://1145.am/db/2984033/Royal_Mail"
        target_node_rels = all_related_uris(ultimate_target_node) # Before we get to deletions

        doc_id = 4001762
        nodes_to_delete = list(Resource.nodes.filter(internalDocId=doc_id))
        uris_to_delete = [x.uri for x in nodes_to_delete]
        merged_nodes = list(Resource.nodes.filter(internalMergedSameAsHighToUri__in=uris_to_delete))
        assert len(nodes_to_delete) > 0
        assert len(merged_nodes) > 0

        # Now load deletion file
        filepath = "integration/test_dump/dump-2/20231224180756/deletions/for_deletion_4001762.ttl"
        load_deletion_file(filepath)
        do_import_ttl(force=True,only_post_processing=True)
        assert len(list(Resource.nodes.filter(internalDocId=doc_id))) == 0 # all deleted
        assert len(list(Resource.nodes.filter(internalMergedSameAsHighToUri__in=uris_to_delete))) == 0

        # And load the file with the new additions
        load_file("integration/test_dump/dump-2/20231224180756/identical_4001762.ttl",RDF_SLEEP_TIME=0)
        do_import_ttl(force=True,only_post_processing=True)
        assert len(list(Resource.nodes.filter(internalDocId=doc_id))) == len(nodes_to_delete)
        # assert len(list(Resource.nodes.filter(internalMergedSameAsHighToUri__in=uris_to_delete))) == len(merged_nodes)

        ultimate_target_node2 = Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail")
        assert ultimate_target_node2.uri == "https://1145.am/db/2984033/Royal_Mail"
        new_vals = all_related_uris(ultimate_target_node2)

        assert len(new_vals) == len(target_node_rels)
        for k in new_vals:
            if k != 'sameAsNameOnly':
                assert new_vals[k] == target_node_rels[k]
        assert target_node_rels['sameAsNameOnly'] - new_vals['sameAsNameOnly'] == {"https://1145.am/db/4001762/Royal_Mail"} # not recreated in the files

    def test_loads_deletion_with_new_edit(self):
        clean_db_and_load_files("integration/test_dump/dump-1",do_post_processing=True)
        do_import_ttl(dirname="integration/test_dump/dump-1.5",force=True,do_archiving=False,do_post_processing=True)
        ultimate_target_node = Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail")
        assert ultimate_target_node.uri == "https://1145.am/db/2984033/Royal_Mail"
        target_node_rels = all_related_uris(ultimate_target_node)
        do_import_ttl(dirname="integration/test_dump/dump-2.1",force=True,do_archiving=False,do_post_processing=True)

        # and check the new company name
        n = Resource.nodes.get_or_none(uri="https://1145.am/db/4001762/Royal_Mail") # search by Resource is faster due to explicit index
        assert isinstance(n, Organization)
        assert n.name == ['Royal Maily Foo Bar'] # name was edited (only updates in the original node)

        ultimate_target_node2 = Organization.self_or_ultimate_target_node(n)
        new_vals = all_related_uris(ultimate_target_node2)

        assert len(new_vals) == len(target_node_rels)
        for k in new_vals:
            if k not in ['sameAsNameOnly','investor']: # investor was added
                assert new_vals[k] == target_node_rels[k]

        assert target_node_rels['investor'] - new_vals['investor'] == set()
        assert new_vals['investor'] - target_node_rels['investor'] == {'https://1145.am/db/4001762/Evri-Acquisition'}

    def test_loads_deletion_without_new_file(self):
        clean_db_and_load_files("integration/test_dump/dump-1",do_post_processing=True)
        do_import_ttl(dirname="integration/test_dump/dump-1.5",force=True,do_archiving=False,do_post_processing=True)
        ultimate_target_node = Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail")
        assert ultimate_target_node.uri == "https://1145.am/db/2984033/Royal_Mail"
        target_node_rels = all_related_uris(ultimate_target_node)
        do_import_ttl(dirname="integration/test_dump/dump-2.2",force=True,do_archiving=False,do_post_processing=True)
        ultimate_target_node2 = Organization.self_or_ultimate_target_node("https://1145.am/db/4001762/Royal_Mail")
        assert ultimate_target_node2 is None
        main_node = Organization.self_or_ultimate_target_node("https://1145.am/db/2984033/Royal_Mail")
        new_vals = all_related_uris(main_node)
        assert len(new_vals) == len(target_node_rels)

        doc_id_in_uri = False
        for v in target_node_rels.values():
            if any([x.find("/4001762/") > 0 for x in v]):
                doc_id_in_uri = True
                break
        assert doc_id_in_uri is True # Originally the main node had relationships that included 4001762 doc id

        doc_id_in_uri = False
        for v in new_vals.values():
            if any([x.find("/4001762/") > 0 for x in v]):
                doc_id_in_uri = True
                break
        assert doc_id_in_uri is False # Should be no occurrence of the deleted doc id in Royal Mail's relationships


class MergeSameAsHighTestCase(TestCase):

    @classmethod
    def setUpTestData(cls):
        clean_db()
        P.nuke_all() # Company name etc are stored in cache
        org_nodes = [make_node(x,y) for x,y in zip(range(100,200),"abcdefghijk")]
        act_nodes = [make_node(x,y,"CorporateFinanceActivity") for x,y in zip(range(100,200),"mnopqrs")]
        node_list = ", ".join(org_nodes + act_nodes)
        query = f"""
            CREATE {node_list},
            (a)<-[:sameAsHigh]-(b),
            (b)-[:sameAsHigh]->(c),
            (c)-[:sameAsHigh]->(d),
            (e)-[:sameAsHigh]->(f),
            (f)<-[:sameAsHigh]-(g),
            (g)-[:sameAsHigh]->(a),

            (h)-[:sameAsHigh]->(i),
            (i)-[:sameAsHigh]->(j),
            (i)<-[:sameAsHigh]-(k),

            (a)-[:buyer]->(m),
            (b)-[:buyer]->(n),
            (c)-[:buyer]->(o),
            (d)-[:investor]->(p),
            (e)-[:investor]->(q),
            (f)-[:buyer]->(r),
            (g)-[:buyer]->(s),

            (m)-[:target]->(h),
            (n)-[:target]->(i),
            (o)-[:target]->(j),
            (p)-[:target]->(k),
            (q)-[:target]->(h),
            (r)-[:target]->(i),
            (s)-[:target]->(j)
        """
        res,_ = db.cypher_query(query)
        R = RDFPostProcessor()
        a = Resource.nodes.get_or_none(uri="https://1145.am/db/100/a")
        assert len(a.buyer) == 1
        assert len(a.investor) == 0
        assert len(a.vendor) == 0
        R.merge_same_as_high_connections()

    def test_merges_all_same_as_highs(self):
        target_uris = []
        merged_uris = []
        for x, y in zip("abcdefghijk",range(100,200)):
            uri = f"https://1145.am/db/{y}/{x}"
            if x in "ah": # target merged nodes
                target_uris.append(uri)
            else:
                merged_uris.append(uri)
        for uri in target_uris:
            logger.info(uri)
            assert Organization.unmerged_or_none_by_uri(uri) is not None
        for uri in merged_uris:
            logger.info(uri)
            assert Resource.nodes.get_or_none(uri=uri) is not None
            assert Organization.unmerged_or_none_by_uri(uri) is None

    def test_attributes_for_ultimate_target(self):
        a = Organization.self_or_ultimate_target_node("https://1145.am/db/100/a")
        assert a.best_name == 'Name A'
        assert a.industry_as_str == 'Baz, Bar'

    def test_gets_ultimate_target(self):
        o = Organization.self_or_ultimate_target_node("https://1145.am/db/105/f")
        assert o.uri == "https://1145.am/db/100/a"
        assert o.best_name == 'Name A'

    def test_merges_connections1(self):
        a = Organization.self_or_ultimate_target_node("https://1145.am/db/101/b") # Actually a
        assert a.uri == "https://1145.am/db/100/a"
        assert len(a.buyer) == 5
        assert len(a.investor) == 2
        assert len(a.vendor) == 0

    def test_merges_connections2(self):
        h = Organization.self_or_ultimate_target_node("https://1145.am/db/109/j") # Actually h
        assert len(h.target) == 7

def clear_neo():
    db.cypher_query("MATCH (n) CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;")


def make_node(doc_id,letter,node_type="Organization",doc_extract=None,datestamp=datetime.now(tz=timezone.utc)):
    if node_type == "Organization":
        industry = "bar" if letter in "aeiou" else "baz"
        industry_str = f"industry: ['{industry}'],"
    else:
        industry_str = ""
    if "Activity" in node_type:
        status_str = "status: ['some status'], "
    else:
        status_str = ""

    node = f"({letter}:Resource:{node_type} {{uri: 'https://1145.am/db/{doc_id}/{letter}', name: ['Name {letter.upper()}'], {industry_str} {status_str} internalDocId: {doc_id}}})"
    if doc_extract is None:
        doc_extract_text = ''
    else:
        doc_extract = doc_extract.replace("'","")
        doc_extract_text = f"documentExtract: '{doc_extract}''"
    doc_source = f"""(docsource_{letter}:Resource:Article {{uri: 'https://1145.am/db/article_{letter}',
                        headline: 'Headline {letter}', sourceOrganization:'prweb', datePublished: datetime('{datestamp.isoformat()}') }})"""
    doc_extract_str = ""
    if "Activity" in node_type:
        doc_extract_str = f"{{ documentExtract: 'Doc Extract {letter.upper()}' }}"
    return f"{node}-[:documentSource {doc_extract_str}]->{doc_source}, (docsource_{letter})-[:url]->(ext_{letter}:Resource {{ uri: 'https://example.org/external/art_{letter}' }})"

def all_related_uris(resource):
    vals = {}
    for k,v in resource.__all_relationships__:
        rel_lens = [x.uri for x in resource.__dict__[k]]
        vals[k] = set(rel_lens)
    return vals
