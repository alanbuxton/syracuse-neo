from topics.models import Resource
from topics.models.models_extras import add_dynamic_classes_for_multiple_labels
from neomodel import db
import logging
from integration.neo4j_utils import (count_relationships, apoc_del_redundant_same_as, get_all_activities_to_merge,
                                     rerun_all_redundant_same_as)
from integration.embedding_utils import create_new_embeddings
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
        RETURN m.uri,n.uri
        ORDER BY m.internalDocId
        LIMIT 500
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
        write_log_header("merge_equivalent_activities")
        self.merge_equivalent_activities()
        write_log_header("merge_same_as_high_connections")
        self.merge_same_as_high_connections()
        write_log_header("Re-merge orgs")
        re_merge_all_orgs_apoc(live_mode=True)
        rerun_all_redundant_same_as()
        write_log_header("adding embeddings")
        create_new_embeddings()
        write_log_header("adding unique resource ids")
        add_resource_ids()


    def merge_equivalent_activities(self, seen_doc_ids=set()):
        acts_to_merge_dict, keep_going, seen_doc_ids = get_all_activities_to_merge(seen_doc_ids)
        for k_uri, vs in acts_to_merge_dict.items():
            if len(vs) == 0:
                logger.warning(f"got {k_uri} for merging into, but nothing to merge into it - unexpected")
                continue
            for v_uri in vs:
                _ = self.merge_nodes(v_uri, k_uri, field_to_update="internalMergedActivityWithSimilarRelationshipsToUri")
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
            for target_node_tmp_uri, source_node_tmp_uri in vals:
                res = self.merge_nodes(source_node_tmp_uri, target_node_tmp_uri)
                if res is False:
                    break
                cnt += 1
                if cnt % 1000 == 0:
                    log_count_relationships(f"After merge_same_as_high_connections {cnt} records")
                if cnt % 100 == 0:
                    logger.info(f"Merged merge_same_as_high_connections {cnt} records")

        log_count_relationships("After merge_same_as_high_connections")

    def merge_nodes(self,source_node_tmp_uri, target_node_tmp_uri, field_to_update="internalMergedSameAsHighToUri"):
        source_node2 = Resource.self_or_ultimate_target_node(source_node_tmp_uri)
        target_node2 = Resource.self_or_ultimate_target_node(target_node_tmp_uri)
        if source_node2 is None or target_node2 is None:
            raise ValueError(f"source: {source_node2} or target: {target_node2} is None")
        logger.debug(f"Merging {source_node_tmp_uri} ({source_node2.uri}) into {target_node_tmp_uri} ({target_node2.uri})")
        if source_node2.internalMergedSameAsHighToUri is not None:
            logger.debug(f"{source_node2.uri} is already merged into {source_node2.internalMergedSameAsHighToUri}, not merging again")
            return False
        if target_node2.internalDocId <= source_node2.internalDocId: # type: ignore
            source_node = source_node2
            target_node = target_node2
        else:
            logger.debug(f"Flipping them round because {source_node2.internalDocId} is lower then {target_node2.internalDocId}")
            source_node = target_node2
            target_node = source_node2
        if source_node.uri == target_node.uri:
            logger.debug("Nodes are already merged to the same target, skipping")
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
        apoc_query = """
        CALL apoc.periodic.iterate(
            "MATCH ()-[rel]-() 
            WHERE rel.weight IS NULL 
            RETURN rel",
            "SET rel.weight = 1",
            {batchSize:1000, parallel:true, retries: 5})
        """
        db.cypher_query(apoc_query)

def write_log_header(message):
    for row in ["*" * 50, message, "*" * 50]:
        logger.info(row)

def log_count_relationships(text):
    cnt = count_relationships()
    logger.info(f"{text}: {cnt} relationships")


def add_resource_ids():
    '''
    In case data consumers don't want to store URIs will create a unique ID for each node
    '''
    query = "MATCH (n: Resource) WHERE n.internalId IS NULL RETURN n"
    action = "SET n.internalId = ID(n)"
    apoc_query = f'CALL apoc.periodic.iterate("{query}","{action}",{{batchSize:1000, parallel:true, retries: 5}})'
    db.cypher_query(apoc_query)
    # Now see if there are any duplicates
    logger.info("Set initial ids, checking for duplicates")
    update_duplicated_resource_ids()

def update_duplicated_resource_ids():
    dup_query = "MATCH (n:Resource) WHERE n.internalId IS NOT NULL WITH n.internalId AS internalId, count(*) AS count WHERE count > 1 RETURN internalId, count"
    dups, _ = db.cypher_query(dup_query,resolve_objects=True)
    if (len(dups) == 0):
        return # No real duplicates
    max_id,_ = db.cypher_query("MATCH (n: Resource) WHERE n.internalId IS NOT NULL RETURN MAX(n.internalId)")
    current_max_id = max_id[0][0]
    if current_max_id is None:
        current_max_id = 0
    for dup in dups:
        dup_id = dup[0]
        if dup_id is None:
            continue # catch-all for non-dups
        query = f"MATCH (n: Resource) WHERE n.internalId = {dup_id} RETURN n.uri ORDER BY n.internalDocId, n.uri "
        vals, _ = db.cypher_query(query)
        for row in vals[1:]:
            current_max_id += 1
            uri = row[0]
            query = f"MATCH (n: Resource {{uri:'{uri}'}}) SET n.internalId = {current_max_id}"
            db.cypher_query(query)
    
def recursively_re_merge_node_via_same_as(source_node,live_mode=False):
    target_uri = source_node.internalMergedSameAsHighToUri
    if target_uri is None:
        return
    target_node = Resource.get_by_uri(target_uri)
    if target_node is None:
        return None
    res = Resource.merge_node_connections(source_node, target_node, run_as_re_merge=True, live_mode=live_mode)
    if res is True:
        recursively_re_merge_node_via_same_as(target_node,live_mode=live_mode)

def re_merge_all_orgs_apoc(live_mode=False):
    logger.info("Started re-merge")
    apoc_query = '''CALL apoc.periodic.iterate(
                        "MATCH (a: Resource&Organization) MATCH (b: Resource&Organization) WHERE a.internalMergedSameAsHighToUri = b.uri RETURN a, b",
                        "MATCH (a)-[r]-(other) 
                        WHERE type(r) <> 'sameAsHigh'
                        AND type(r) <> 'industryClusterSecondary'
                        AND type(r) <> 'documentSource'
                        AND other.internalMergedActivityWithSimilarRelationshipsToUri IS NULL
                        AND NOT EXISTS {MATCH (b)-[r2]-(other) WHERE type(r2) = type(r)}
                        SET a.re_merge_candidate = true",
                        {batchSize: 100, parallel: true}
                        )
                        '''   
    _ = db.cypher_query(apoc_query)
    logger.info("Finished apoc query")
    process_batch_of_re_merge_candidates(live_mode=live_mode)

def process_batch_of_re_merge_candidates(live_mode=False, limit=1000):
    logger.info(f"processing batch of re-merge candidates: live_mode {live_mode}, limit {limit}")
    orgs, _ = db.cypher_query(f"MATCH (a: Resource) WHERE a.re_merge_candidate = true AND a.internalDocId > 0 RETURN a ORDER BY a.internalDocId DESC LIMIT {limit}", 
                              resolve_objects=True) 
    uris = []
    for org_row in orgs:
        org = org_row[0]
        uris.append(org.uri)
        recursively_re_merge_node_via_same_as(org,live_mode=live_mode)
    _ = db.cypher_query(f"MATCH (a: Resource) WHERE a.uri in {uris} REMOVE a.re_merge_candidate")
    logger.info(f"Processed {len(orgs)} records")
    if len(orgs) > 0:
        process_batch_of_re_merge_candidates(live_mode=live_mode, limit=limit)
