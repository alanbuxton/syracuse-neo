import logging
from neomodel import db
import re
import time
from topics.models import Resource

logger = logging.getLogger(__name__)

def delete_and_clean_up_nodes_from_doc_id_file(doc_id_file):
    with open(doc_id_file, "r") as f:
        doc_ids = f.readlines()
    for row in doc_ids:
        try:
            doc_id = int(row.strip())
            logger.info(f"Deleting internalDocId {doc_id}")
            delete_and_clean_up_nodes_by_doc_id(doc_id)
        except:
            logger.info(f"Couldn't do anything with internalDocID {row}")

def setup_db_if_necessary():
    db.cypher_query("CREATE CONSTRAINT n10s_unique_uri IF NOT EXISTS FOR (r:Resource) REQUIRE r.uri IS UNIQUE;")
    db.cypher_query("CREATE INDEX node_internal_doc_id_index IF NOT EXISTS FOR (n:Resource) on (n.internalDocId)")
    db.cypher_query("CREATE INDEX node_merged_same_as_high_to_uri IF NOT EXISTS FOR (n:Resource) on (n.internalMergedSameAsHighToUri)")
    db.cypher_query("CREATE FULLTEXT INDEX resource_names IF NOT EXISTS FOR (r:Resource) ON EACH [r.name]")
    db.cypher_query("CREATE INDEX article_date_published IF NOT EXISTS FOR (a:Article) ON (a.datePublished)")
    db.cypher_query("CREATE INDEX geonames_location_country_admin1_index IF NOT EXISTS FOR (n: GeoNamesLocation) on (n.countryCode, n.admin1Code)")
    db.cypher_query("CREATE VECTOR INDEX industry_cluster_representative_docs_vec IF NOT EXISTS FOR (i: IndustryCluster) ON i.representative_doc_embedding OPTIONS { indexConfig: { `vector.dimensions`: 768, `vector.similarity_function`: 'cosine' } }")
    db.cypher_query("CREATE VECTOR INDEX organization_industries_vec IF NOT EXISTS FOR (n:Organization) ON n.industry_embedding OPTIONS { indexConfig: { `vector.dimensions`: 768, `vector.similarity_function`: 'cosine' } }")
    v, _ = db.cypher_query("call n10s.graphconfig.show")
    if len(v) == 0:
        do_n10s_config()
    else:
        do_n10s_config(overwrite=True)

def do_n10s_config(overwrite=False):
    multivals = ["actionFoundName","activityType","basedInHighClean",
                "basedInHighGeoName",
                "basedInHighRaw","basedInLowRaw",
                "description","foundName","industry",
                "locationFoundName",
                "locationPurpose","locationType","name",
                "nameClean", "orgName",
                "orgFoundName",
                "productName",
                "roleFoundName","roleHolderFoundName",
                "status","targetDetails","targetName",
                "useCase",
                "valueRaw",
                "when","whenRaw","whereHighGeoName","whereHighRaw","whereHighClean",
                # IndustryCluster
                "representation","representativeDoc",
                ]
    proplist = [f"https://1145.am/db/{x}" for x in multivals]
    params = 'handleVocabUris: "MAP",handleMultival:"ARRAY",multivalPropList:["' + "\",\"".join(proplist) + '"]'
    if overwrite is True:
        query = 'CALL n10s.graphconfig.set({' + params + ', force: true })'
    else:
        query = 'CALL n10s.graphconfig.init({' + params + '})'
    logger.info(query)
    db.cypher_query(query)

def apoc_del_redundant_same_as():
    ts = time.time()
    output_same_as_stats("Before delete")
    apoc_query_high = f'CALL apoc.periodic.iterate("MATCH (n1:Resource)-[r1:sameAsHigh]->(n2:Resource)-[r2:sameAsHigh]->(n1) where elementId(n1) < elementId(n2) AND n1.deletedRedundantSameAsAt IS NULL AND n2.deletedRedundantSameAsAt IS NULL RETURN *","DELETE r2",{{}})'
    db.cypher_query(apoc_query_high)
    apoc_query_medium = f'CALL apoc.periodic.iterate("MATCH (n1:Resource)-[r1:sameAsNameOnly]->(n2:Resource)-[r2:sameAsNameOnly]->(n1) where elementId(n1) < elementId(n2) AND n1.deletedRedundantSameAsAt IS NULL AND n2.deletedRedundantSameAsAt IS NULL RETURN *","DELETE r2",{{}})'
    db.cypher_query(apoc_query_medium)
    apoc_query_set_flag = f'''CALL apoc.periodic.iterate("MATCH (n1:Resource) WHERE n1.deletedRedundantSameAsAt IS NULL RETURN *","SET n1.deletedRedundantSameAsAt = {ts}",{{}})'''
    db.cypher_query(apoc_query_set_flag)
    output_same_as_stats("After Delete sameAsNameOnly")

def delete_all_not_needed_resources():
    query = """MATCH (n: Resource) WHERE n.uri CONTAINS 'https://1145.am/db/'
            AND SIZE(LABELS(n)) = 1
            CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;"""
    db.cypher_query(query)

def output_same_as_stats(msg):
    high = "MATCH (n1)-[r:sameAsHigh]-(n2)"
    medium = "MATCH (n1)-[r:sameAsNameOnly]-(n2)"
    same_as_high_count,_ = db.cypher_query(high + " RETURN COUNT(r)")
    same_as_medium_count,_ = db.cypher_query(medium + " RETURN COUNT(r)")
    logger.info(f"{msg} sameAsHigh: {same_as_high_count[0][0]}; sameAsNameOnly: {same_as_medium_count[0][0]}")

def get_internal_doc_ids_from_rdf_row(row):
    res = re.findall(r"^\s+ns1:internalDocId\s(\d+)", row)
    if len(res) > 0:
        return int(res[0])
    else:
        return None

def get_node_name_from_rdf_row(row):
    res = re.findall(r"^<(https://\S.+)> a", row)
    if len(res) > 0:
        return res[0]
    else:
        return None

def delete_and_clean_up_nodes_by_doc_id(doc_id):
    nodes_to_delete = Resource.nodes.filter(internalDocId=doc_id)
    uris_to_delete = [x.uri for x in nodes_to_delete]
    merged_nodes = Resource.nodes.filter(internalMergedSameAsHighToUri__in=uris_to_delete)
    for n in nodes_to_delete:
        n.delete()
    for m in merged_nodes:
        m.internalMergedSameAsHighToUri = None
        m.save()

def count_relationships():
    vals, _ = db.cypher_query("MATCH ()-[x]-() RETURN COUNT(x);")
    cnt = vals[0][0]
    return cnt

def count_nodes():
    val, _ = db.cypher_query("MATCH (n) RETURN COUNT(n)")
    return val[0][0]
