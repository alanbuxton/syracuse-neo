from neomodel import db
from integration.neo4j_utils import output_same_as_stats
import logging
logger = logging.getLogger(__name__)

# It's only Organizations that have sameAsHigh, but all resources will hvae internal_doc_id so
# creating index at Resource level. This means that the merge query needs to refer to Resource or index won't be used
merge_query = """
    MATCH (m: Resource)-[x:sameAsHigh]-(n: Resource)
    WHERE m.internalDocId < n.internalDocId
    WITH m, n
    LIMIT 1
    SET m.sameAsHighUri = m.uri
    SET n.sameAsHighUri = n.uri
    WITH head(collect([m,n])) as nodes
    CALL apoc.refactor.mergeNodes(nodes, { properties: {
        uri: "discard",
        `.*`: "combine"
        },
        produceSelfRel: false,
        mergeRels:true }) YIELD node
    RETURN node
"""

def merge_same_as_high():
    cnt = 0
    logger.info("Starting merge sameAsHigh")
    db.cypher_query("CREATE INDEX node_internal_doc_id_index IF NOT EXISTS FOR for (o:Resource) on (o.internalDocId)")
    while True:
        a,b = db.cypher_query(merge_query)
        logger.debug(a)
        if len(a) == 0:
            break
        cnt += 1
        if cnt % 1000 == 0:
            output_same_as_stats(f"After merging {cnt} records")
        if cnt % 100 == 0:
            logger.info(f"Merged {cnt} records")
    output_same_as_stats("After merge sameAsHigh")
