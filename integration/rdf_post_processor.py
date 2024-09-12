from topics.models import Resource
from neomodel import db
import logging
from integration.neo4j_utils import count_relationships, apoc_del_redundant_same_as
logger = logging.getLogger(__name__)

class RDFPostProcessor(object):

    # m is target (that we are going to merge to), n is source (that we are copying relationships from)
    QUERY_SAME_AS_HIGH_FOR_MERGE = f"""
        MATCH (m: Resource)-[x:sameAsHigh]-(n: Resource)
        WHERE m.internalDocId <= n.internalDocId
        AND m.internalMergedSameAsHighToUri IS NULL
        AND n.internalMergedSameAsHighToUri IS NULL
        AND LABELS(m) = LABELS(n)
        AND NOT "CorporateFinanceActivity" IN LABELS(m)
        AND NOT "RoleActivity" IN LABELS(m)
        AND NOT "LocationActivity" IN LABELS(m)
        RETURN m,n
        ORDER BY m.internalDocId
        LIMIT 1000
    """

    QUERY_SAME_AS_HIGH_FOR_MERGE_COUNT = f"""
        MATCH (m: Resource)-[x:sameAsHigh]-(n: Resource)
        WHERE m.internalDocId <= n.internalDocId
        AND m.internalMergedSameAsHighToUri IS NULL
        AND n.internalMergedSameAsHighToUri IS NULL
        AND LABELS(m) = LABELS(n)
        AND NOT ANY(x in LABELS(n) WHERE x =~ ".+Activity")
        RETURN count(*)
    """

    QUERY_SELF_RELATIONSHIP = f"""
        MATCH (n: Resource)-[r]-(n)
        DELETE r
        RETURN *
    """

    def run_all_in_order(self):
        apoc_del_redundant_same_as()
        write_log_header("delete_self_relationships")
        self.delete_self_relationships()
        write_log_header("add_document_extract_to_relationship")
        self.add_document_extract_to_relationship()
        write_log_header("merge_same_as_high_connections")
        self.merge_same_as_high_connections()

    def delete_self_relationships(self):
        res, _ = db.cypher_query(self.QUERY_SELF_RELATIONSHIP)
        logger.info(f"Deleted {len(res)} self-relationships")

    def merge_same_as_high_connections(self):
        cnt = 0
        log_count_relationships("Before merge_same_as_high_connections")
        while True:
            logger.info("Querying for entries to merge")
            vals,_ = db.cypher_query(self.QUERY_SAME_AS_HIGH_FOR_MERGE,
                                        resolve_objects=True)
            if len(vals) == 0:
                break
            for target_node_tmp, source_node_tmp in vals:
                res = self.merge_nodes(source_node_tmp, target_node_tmp)
                if res is False:
                    break
                cnt += 1
                if cnt % 1000 == 0:
                    log_count_relationships(f"After merge_same_as_high_connections {cnt} records")
                if cnt % 100 == 0:
                    logger.info(f"Merged merge_same_as_high_connections {cnt} records")

        log_count_relationships("After merge_same_as_high_connections")

    def merge_nodes(self,source_node_tmp, target_node_tmp):
        source_node2 = Resource.self_or_ultimate_target_node(source_node_tmp.uri)
        target_node2 = Resource.self_or_ultimate_target_node(target_node_tmp.uri)
        logger.info(f"Merging {source_node_tmp.uri} ({source_node2.uri}) into {target_node_tmp.uri} ({target_node2.uri})")
        if source_node2.internalMergedSameAsHighToUri is not None:
            logger.info(f"{source_node2.uri} is already merged into {source_node2.internalMergedSameAsHighToUri}, not merging again")
            return False
        if target_node2.internalDocId <= source_node2.internalDocId:
            source_node = source_node2
            target_node = target_node2
        else:
            logger.info(f"Flipping them round because {source_node2.internalDocId} is lower then {target_node2.internalDocId}")
            source_node = target_node2
            target_node = source_node2
        if source_node.uri == target_node.uri:
            logger.info("Nodes are already merged to the same target, skipping")
            return False
        Resource.merge_node_connections(source_node,target_node)
        return True


    def add_document_extract_to_relationship(self):
        logger.info("Moving document extract to relationship")
        query = """
            MATCH (n:Resource)-[d:documentSource]->(a:Article)
            WHERE d.documentExtract IS NULL
            AND n.documentExtract IS NOT NULL
            CALL {
                WITH d, n
                SET d.documentExtract = n.documentExtract
            }
            IN TRANSACTIONS OF 10000 ROWS;
            """
        db.cypher_query(query)

def write_log_header(message):
    for row in ["*" * 50, message, "*" * 50]:
        logger.info(row)

def log_count_relationships(text):
    cnt = count_relationships()
    logger.info(f"{text}: {cnt} relationships")
