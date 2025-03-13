from topics.models import Resource
from topics.models.models_extras import add_dynamic_classes_for_multiple_labels
from neomodel import db
import logging
from integration.neo4j_utils import count_relationships, apoc_del_redundant_same_as, get_all_activities_to_merge
from integration.vector_search_utils import create_new_embeddings
import time
logger = logging.getLogger(__name__)

class RDFPostProcessor(object):

    # m is target (that we are going to merge to), n is source (that we are copying relationships from)
    QUERY_SAME_AS_HIGH_FOR_MERGE = f"""
        MATCH (m: Resource)-[x:sameAsHigh]-(n: Resource)
        WHERE m.internalDocId <= n.internalDocId
        AND m.internalMergedSameAsHighToUri IS NULL
        AND n.internalMergedSameAsHighToUri IS NULL
        AND LABELS(m) = LABELS(n)
        AND "Organization" IN LABELS(m)
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
        write_log_header("Creating multi-inheritance classes")
        add_dynamic_classes_for_multiple_labels(ignore_cache=True)
        write_log_header("del_redundant_same_as")
        apoc_del_redundant_same_as()
        write_log_header("delete_self_relationships")
        self.delete_self_relationships()
        write_log_header("add_document_extract_to_relationship")
        self.add_document_extract_to_relationship()
        write_log_header("Set default weighting to 1")
        self.add_weighting_to_relationship()
        write_log_header("Creating multi-inheritance classes")
        add_dynamic_classes_for_multiple_labels()
        write_log_header("merge_same_as_high_connections")
        self.merge_same_as_high_connections()
        write_log_header("merge_equivalent_activities")
        self.merge_equivalent_activities()
        write_log_header("adding embeddings")
        create_new_embeddings()


    def merge_equivalent_activities(self, seen_doc_ids=set()):
        acts_to_merge_dict, keep_going, seen_doc_ids = get_all_activities_to_merge(seen_doc_ids)
        for k_uri, vs in acts_to_merge_dict.items():
            k = Resource.get_by_uri(k_uri)
            if len(vs) == 0:
                logger.warning(f"got {k_uri} for merging into, but nothing to merge into it - unexpected")
                continue
            for v_uri in vs:
                v = Resource.get_by_uri(v_uri)
                _ = self.merge_nodes(v, k, field_to_update="internalMergedActivityWithSimilarRelationshipsToUri")
        if keep_going is True:
            seen_doc_ids = self.merge_equivalent_activities(seen_doc_ids)
        else:
            self.mark_as_updated_merge_equivalent_activities(seen_doc_ids)

    def mark_as_updated_merge_equivalent_activities(self,seen_doc_ids):
        logger.info(f"Marking updated with {len(seen_doc_ids)} internal doc ids")
        ts = time.time()
        query = f"""
            MATCH (n:Resource)
            WHERE n.internalDocId in {list(seen_doc_ids)} AND ANY(x in LABELS(n) WHERE x =~ ".+Activity")
            CALL {{
                WITH n
                SET n.internalMergedActivityWithSimilarRelationshipsAt = {ts}
            }}
            IN TRANSACTIONS OF 10000 ROWS;
            """
        db.cypher_query(query)

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

    def merge_nodes(self,source_node_tmp, target_node_tmp, field_to_update="internalMergedSameAsHighToUri"):
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
        Resource.merge_node_connections(source_node,target_node,field_to_update=field_to_update)
        return True
    
    def add_document_extract_to_relationship(self):
        logger.info("Adding document extract to relationship")
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

    def add_weighting_to_relationship(self):
        logger.info("Adding weighting to relationship")
        query = "MATCH (n: Resource)-[rel]-(o:Resource) WHERE rel.weight is NULL RETURN rel"
        action = "SET rel.weight = 1"
        apoc_query = f'CALL apoc.periodic.iterate("{query}","{action}",{{}})'
        db.cypher_query(apoc_query)

def write_log_header(message):
    for row in ["*" * 50, message, "*" * 50]:
        logger.info(row)

def log_count_relationships(text):
    cnt = count_relationships()
    logger.info(f"{text}: {cnt} relationships")
