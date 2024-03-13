from neomodel import db
from integration.neo4j_utils import output_same_as_stats, apoc_del_redundant_med
import logging
logger = logging.getLogger(__name__)

merge_same_as_high_query = """
    MATCH (m: Resource)-[x:sameAsHigh]-(n: Resource)
    WHERE m.internalDocId <= n.internalDocId
    WITH m, n
    LIMIT 1
    SET m.sameAsHighUri = m.uri
    SET n.sameAsHighUri = n.uri
    SET m.internalDocIdList = m.internalDocId
    SET n.internalDocIdList = n.internalDocId
    WITH m, n
    UNWIND(n.name) as name
    WITH n, m, COLLECT(n.internalDocId + "_##_" + name) as name_list
    SET n.name_list = name_list
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
    AND m.internalDocId IS NOT NULL // Much faster if being explicit about index
    AND n.internalDocId IS NOT NULL
    WITH n, m, count(m) as count_of_merged
    WHERE count_of_merged = 1
    SET n.sameAsMediumUri = n.uri
    SET n.internalDocIdList = n.internalDocId
    WITH m, n
    UNWIND(n.name) as name
    WITH n, m, COLLECT(n.internalDocId + "_##_" + name) as name_list
    SET n.name_list = name_list
    return m, n
    """

def post_import_merging():
    merge_same_as_high()
    apoc_del_redundant_med()
    merge_same_as_medium()

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

def merge_same_as_medium():
    '''
        If this medium is connected to one merged node (i.e. to some 'sameAsHigh' that have been merged), then merge the
        sameAsMedium as well.

        There may be cases where a similar name is a different company, so if all of the sameAs point to the same company
        we can use it to merge to. If there are disagreements in the sameAsMedium piece then don't change anything
    '''
    cnt = 0
    logger.info("Starting merge sameAsMediumUri")
    nodes_to_merge, _ = db.cypher_query(find_same_as_mediums_for_merge_query, resolve_objects=True)
    for base_node, node_to_attach in nodes_to_merge:
        merge_query = create_merge_nodes_query(base_node.uri, node_to_attach.uri)
        res = db.cypher_query(merge_query)
        cnt += 1
        if cnt % 1000 == 0:
            output_same_as_stats(f"After merging {cnt} sameAsMedium records")
        if cnt % 100 == 0:
            logger.info(f"Merged {cnt} sameAsMedium records")
    output_same_as_stats("After merge sameAsMedium")
    return nodes_to_merge


def merge_same_as_high():
    cnt = 0
    logger.info("Starting merge sameAsHigh")
    db.cypher_query("CREATE INDEX node_internal_doc_id_index IF NOT EXISTS FOR (n:Resource) on (n.internalDocId)")
    while True:
        a,b = db.cypher_query(merge_same_as_high_query)
        logger.debug(a)
        if len(a) == 0:
            break
        cnt += 1
        if cnt % 1000 == 0:
            output_same_as_stats(f"After merging {cnt} sameAsHigh records")
        if cnt % 100 == 0:
            logger.info(f"Merged {cnt} sameAsHigh records")
    output_same_as_stats("After merge sameAsHigh")
