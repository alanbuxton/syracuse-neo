import logging
from neomodel import db
import re
import time
from topics.models import Resource
from collections import defaultdict

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
    db.cypher_query("CREATE INDEX resource_internalMergedActivityWithSimilarRelationshipsToUri IF NOT EXISTS FOR (n:Resource) on (n.internalMergedActivityWithSimilarRelationshipsToUri)")
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

def get_potential_duplicate_activities():
    query = f"""MATCH (a: Resource)-[:documentSource]->(d: Resource&Article)<-[:documentSource]-(b: Resource) 
        WHERE a.internalDocId = b.internalDocId
        AND ANY(x in LABELS(a) WHERE x =~ ".+Activity")
        AND LABELS(b) = LABELS(a)
        AND NOT a: Organization
        AND NOT a: Person
        AND NOT a: Role
        AND NOT a: Site
        AND (a.internalMergedActivityWithSimilarRelationshipsAt IS NULL AND b.internalMergedActivityWithSimilarRelationshipsAt IS NULL)
        AND a.uri < b.uri
        AND (a.internalMergedActivityWithSimilarRelationshipsToUri IS NULL AND b.internalMergedActivityWithSimilarRelationshipsToUri IS NULL)
        RETURN DISTINCT a.uri, b.uri, b.internalDocId 
        ORDER BY b.internalDocId, a.uri, b.uri
        """
    res, _ = db.cypher_query(query)
    return res

def related_orgs_query(uri):
    query = f"""MATCH (a: Resource {{uri:'{uri}'}})-[r]-(e: Organization)
                WHERE e.internalMergedSameAsHighToUri IS NULL
                RETURN distinct type(r), e.uri"""
    vals, _ = db.cypher_query(query)
    return vals

def related_roles_query(uri):
    query = f"""MATCH (p: Person)-[ra:roleActivity]-(a: Resource {{uri:'{uri}'}})-[r:role]-(e: Role)
                WHERE e.internalMergedSameAsHighToUri IS NULL
                RETURN distinct p.uri, e.uri"""
    vals, _ = db.cypher_query(query)
    return vals

def related_orgs_or_roles(uri):
    vals = related_orgs_query(uri) + related_roles_query(uri)
    vals = set([ (t,uri) for (t,uri) in vals])
    return vals

def superset_node(uri_a, uri_b):
    '''
        Returns superset node uri, subset node uri
        or None, None if neither is a subset
        If both are the same it treats uri_a as the superset
        None if neither node is a subseet
    '''
    rels_a = related_orgs_or_roles(uri_a)
    rels_b = related_orgs_or_roles(uri_b)
    if rels_b.issubset(rels_a):
        return uri_a, uri_b
    elif rels_a.issubset(rels_b):
        return uri_b, uri_a  
    return None, None

def get_all_activities_to_merge(seen_doc_ids):
    '''
        Returns activities dict and then 
        either "True" if bailed early so worth trying again or "False" if there's nothing more to see, and
        list of seen uris
    '''
    logger.info("Starting get_all_activities_to_merge")
    activities_to_merge = defaultdict(set)
    activities = get_potential_duplicate_activities()
    logger.info(f"found {len(activities)} activities to potentially merge")
    seen_subs = set()
    keep_going = False
    cnt = 0
    for uri_a, uri_b, doc_id in activities:
        seen_doc_ids.add(doc_id)
        super_n, sub_n = superset_node(uri_a, uri_b)
        if super_n is not None:
            if sub_n in seen_subs:
                logger.debug(f"Want to merge {sub_n} into {super_n} but we've already seen {sub_n}. All activities_to_merge: {activities_to_merge}")
                keep_going = True
                continue
            if super_n in seen_subs:
                logger.debug(f"Want to merge {sub_n} into {super_n} but {super_n} is already merged elsewhere. All activities_to_merge: {activities_to_merge}")
                keep_going = True
                continue
            activities_to_merge[super_n].add(sub_n)
            seen_subs.add(sub_n)
        else:
            logger.debug(f"No subset/superset relationship between {uri_a} and {uri_b}")
        cnt += 1
        if cnt % 1000 == 0:
            logger.info(f"Processed {cnt} rows")
    return activities_to_merge, keep_going, seen_doc_ids


