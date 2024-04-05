from neomodel import db
from integration.neo4j_utils import output_same_as_stats, apoc_del_redundant_same_as
import logging
logger = logging.getLogger(__name__)

merge_same_as_high_query = """
    MATCH (m: Resource)-[x:sameAsHigh]-(n: Resource)
    WHERE m.internalDocId <= n.internalDocId
    AND LABELS(m) = LABELS(n)
    AND NOT ANY(x in LABELS(n) WHERE x =~ ".+Activity")
    AND m.uri <> n.uri
    WITH m, n
    LIMIT 1
    UNWIND(n.name) as n_name
    UNWIND(m.name) as m_name
    WITH n, m, COLLECT(n.internalDocId + "_##_" + n_name) as n_name,
               COLLECT(m.internalDocId + "_##_" + m_name) as m_name
    SET n.internalSameAsHighUriList = COALESCE(n.internalSameAsHighUriList,[]) + [n.uri]
    SET m.internalSameAsHighUriList = COALESCE(m.internalSameAsHighUriList,[]) + [m.uri]
    SET m.internalDocIdList = COALESCE(m.internalDocIdList,[m.internalDocId])
    SET n.internalDocIdList = COALESCE(n.internalDocIdList,[]) + [n.internalDocId]
    SET n.internalNameList = COALESCE(n.internalNameList, n_name)
    SET m.internalNameList = COALESCE(m.internalNameList, m_name)
    WITH head(collect([m,n])) as nodes
    CALL apoc.refactor.mergeNodes(nodes, { properties: {
        uri: "discard",
        internalDocId: "discard",
        `.*`: "combine"
        },
        produceSelfRel: false,
        mergeRels:true,
        singleElementAsArray: true }) YIELD node
    WITH node
    SET node.merged = True
    RETURN node
"""

find_same_as_mediums_for_merge_query = """
    MATCH (n: Resource)-[r:sameAsMedium]-(m:Resource)
    WHERE m.merged = True AND n.merged IS NULL
    AND LABELS(n) = LABELS(m)
    AND NOT ANY(x in LABELS(n) WHERE x =~ ".+Activity")
    AND m.internalDocId IS NOT NULL // Much faster if being explicit about index
    AND n.internalDocId IS NOT NULL
    AND n.uri <> m.uri
    WITH n, m, count(m) as count_of_merged
    WHERE count_of_merged = 1
    SET n.internalSameAsMediumUriList = COALESCE(n.internalSameAsMediumUriList,[]) + [n.uri]
    SET n.internalDocIdList = COALESCE(n.internalDocIdList,[]) + [n.internalDocId]
    WITH m, n
    UNWIND(n.name) as name
    WITH n, m, COLLECT(n.internalDocId + "_##_" + name) as nameList
    SET n.internalNameList = nameList
    return m, n
    """

def create_merge_activities_query(rel1="-[:investor]->",
                                joiner="CorporateFinanceActivity",
                                rel2="-[:target]->"):
    return f"""
    MATCH (r1: Resource){rel1}(j: {joiner}){rel2}(r2: Resource)
    WITH r1, r2, j
    ORDER BY j.internalDocId
    WITH r1, r2, COLLECT(j) AS js
    WHERE SIZE(js) > 1
    WITH js
    LIMIT 1
    UNWIND(js) as n
    WITH n
    SET n.internalDocIdList = COALESCE(n.internalDocIdList,[]) + [n.internalDocId]
    SET n.internalActivityList = COALESCE(n.internalActivityUriList,[]) + [n.uri]
    WITH COLLECT(n) as nodes
    CALL apoc.refactor.mergeNodes(nodes, {{ properties: {{
        uri: "discard",
        internalDocId: "discard",
        `.*`: "combine"
        }},
        produceSelfRel: false,
        mergeRels:true,
        singleElementAsArray: true }}) YIELD node
    RETURN node
    """

def post_import_merging(with_delete_not_needed_resources=False):
    apoc_del_redundant_same_as()
    reallocate_same_as_to_already_merged_nodes()
    merge_same_as_high()
    apoc_del_redundant_same_as()
    merge_same_as_medium()
    if with_delete_not_needed_resources is True:
        delete_all_not_needed_resources()
    delete_self_same_as()
    move_document_extract_to_relationship()
    merge_all_corp_fin_activities()
    apoc_del_redundant_same_as()

def move_document_extract_to_relationship():
    logger.info("Moving document extract to relationship")
    query = """
        MATCH (n:CorporateFinanceActivity|LocationActivity|RoleActivity)-[d:documentSource]->(a:Article)
        WHERE d.documentExtract IS NULL
        CALL {
            WITH n, d
            SET d.documentExtract = n.documentExtract
            REMOVE n.documentExtract
        }
        IN TRANSACTIONS OF 10000 ROWS;
        """
    db.cypher_query(query)

def delete_self_same_as():
    query = "MATCH (a: Organization)-[rel:sameAsHigh|sameAsMedium]->(a) DELETE rel;"
    db.cypher_query(query)


def delete_all_not_needed_resources():
    query = """MATCH (n: Resource) WHERE n.uri CONTAINS 'https://1145.am/db/'
            AND SIZE(LABELS(n)) = 1
            CALL {WITH n DETACH DELETE n} IN TRANSACTIONS OF 10000 ROWS;"""
    db.cypher_query(query)

def create_merge_nodes_query(base_uri, attach_uri):
    merge_nodes_query=f"""
        MATCH (m: Resource {{uri:'{base_uri}' }})--(n: Resource {{uri:'{attach_uri}'}})
        WITH head(collect([m,n])) as nodes
        CALL apoc.refactor.mergeNodes(nodes, {{ properties: {{
            uri: "discard",
            internalDocId: "discard",
            `.*`: "combine"
            }},
            produceSelfRel: false,
            mergeRels:true,
            singleElementAsArray: true }}) YIELD node
        RETURN node
    """
    return merge_nodes_query

def merge_all_corp_fin_activities():
    cnt = 0
    logger.info("Starting merge activities")
    match_strs = [
        ("-[:investor]->","-[:target]->"),
        ("-[:buyer]->","-[:target]->"),
        ("<-[:vendor]-","-[:target]->"),
    ]
    for rel1,rel2 in match_strs:
        res = merge_activities_for(rel1,"CorporateFinanceActivity",rel2)
        cnt += res
    logger.info(f"Merged {cnt} records")

def merge_activities_for(rel1,joiner,rel2):
    cnt = 0
    current_count,_ = db.cypher_query(f"MATCH (n: {joiner}) RETURN COUNT(n);")
    logger.info(f"Merging Activities for {rel1}{joiner}{rel2} - currently {current_count[0][0]}")
    while True:
        query = create_merge_activities_query(rel1,joiner,rel2)
        a,_ = db.cypher_query(query)
        logger.debug(f"Merged {a}")
        if len(a) == 0:
            break
        cnt += 1
        if cnt % 1000 == 0:
            current_count,_ = db.cypher_query(f"MATCH (n: {joiner}) RETURN COUNT(n);")
            logger.info(f"Merged {cnt} {joiner} records, now {current_count[0][0]};")
        elif cnt % 100 == 0:
            logger.info(f"Merged {cnt} {joiner} records")
    return cnt

def merge_same_as_medium():
    '''
        If this medium is connected to one merged node (i.e. to some 'sameAsHigh' that have been merged), then merge the
        sameAsMedium as well.

        There may be cases where a similar name is a different company, so if all of the sameAs point to the same company
        we can use it to merge to. If there are disagreements in the sameAsMedium piece then don't change anything
    '''
    cnt = 0
    logger.info("Starting merge sameAsMediumUri")
    while True:
        nodes_to_merge, _ = db.cypher_query(find_same_as_mediums_for_merge_query, resolve_objects=True)
        if len(nodes_to_merge) == 0:
            break
        for base_node, node_to_attach in nodes_to_merge:
            merge_query = create_merge_nodes_query(base_node.uri, node_to_attach.uri)
            res = db.cypher_query(merge_query)
            cnt += 1
            if cnt % 1000 == 0:
                output_same_as_stats(f"After merging {cnt} sameAsMedium records")
            if cnt % 100 == 0:
                logger.info(f"Merged {cnt} sameAsMedium records")
    output_same_as_stats("After merge sameAsMedium")

def merge_same_as_high():
    cnt = 0
    logger.info("Starting merge sameAsHigh")
    db.cypher_query("CREATE INDEX node_internal_doc_id_index IF NOT EXISTS FOR (n:Resource) on (n.internalDocId)")
    while True:
        a,b = db.cypher_query(merge_same_as_high_query)
        logger.debug(f"Merged {a}")
        if len(a) == 0:
            break
        cnt += 1
        if cnt % 1000 == 0:
            output_same_as_stats(f"After merging {cnt} sameAsHigh records")
        if cnt % 100 == 0:
            logger.info(f"Merged {cnt} sameAsHigh records")
    output_same_as_stats("After merge sameAsHigh")

def reallocate_same_as_to_already_merged_nodes():
    cnt_merged = 0
    cnt_deleted = 0
    next_log = 100
    logger.info("Starting reallocate_same_as_to_already_merged_nodes")

    bare_resources_query = """MATCH (target: Resource)-[x:sameAsHigh|sameAsMedium]-(source: Resource)
        WHERE target.internalDocId IS NULL
        AND source.internalDocId IS NOT NULL
        AND SIZE(LABELS(target)) = 1
        AND SIZE(LABELS(source)) > 1
        AND source.uri <> target.uri
        RETURN DISTINCT(target)"""

    res,_ = db.cypher_query(bare_resources_query, resolve_objects=True)
    for row in res:
        target = row[0]
        merged_entity = target.get_merged_same_as_high_by_unmerged_uri()
        if merged_entity is None:
            logger.debug(f"No merged entity for {target.uri}, will delete")
            target.delete()
            cnt_deleted += 1
        else:
            for source in target.sameAsHigh:
                source.sameAsHigh.connect(merged_entity)
                source.sameAsHigh.disconnect(target)
                cnt_merged += 1
            for source in target.sameAsMedium:
                source.sameAsMedium.connect(merged_entity)
                source.sameAsMedium.disconnect(target)
                cnt_merged += 1
            target.delete()
        if cnt_merged >= next_log:
            output_same_as_stats(f"Reallocated {cnt_merged} records")
            next_log = next_log + 100
    output_same_as_stats(f"Reallocated {cnt_merged} records; deleted {cnt_deleted} records")
