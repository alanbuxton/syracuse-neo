from topics.models import Resource, Organization
from topics.models.models_extras import add_dynamic_classes_for_multiple_labels
from neomodel import db
import logging
from integration.neo4j_utils import (count_relationships, apoc_del_redundant_same_as, get_all_activities_to_merge,
                                     rerun_all_redundant_same_as)
from integration.embedding_utils import create_new_embeddings
import time
from typing import Tuple, Union
logger = logging.getLogger(__name__)

class RDFPostProcessor(object):

    GCC_CREATE_SAME_AS="""CALL gds.graph.project(
        'sameAsGraph',
        'Organization',
        {
            SAME_AS: {
                type: 'sameAsHigh',
                orientation: 'UNDIRECTED'
            }
        })"""
    
    GCC_WRITE_SAME_AS_COMPONENTS="""
        CALL gds.wcc.write('sameAsGraph', {
            writeProperty: 'componentId'
        })
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
        add_dynamic_classes_for_multiple_labels(ignore_cache=True)
        write_log_header("merge_equivalent_activities")
        self.merge_equivalent_activities()
        write_log_header("merge_same_as_high_connections")
        self.merge_same_as_high_connections()
        write_log_header("redundant same_as")
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
        _ = db.cypher_query("CALL gds.graph.drop('sameAsGraph', false)") # just in case it's still there
        _ = db.cypher_query(self.GCC_CREATE_SAME_AS)
        _ = db.cypher_query(self.GCC_WRITE_SAME_AS_COMPONENTS)
        components, _ = db.cypher_query("MATCH (n: Organization) WHERE n.componentId IS NOT NULL RETURN DISTINCT(n.componentId)")
        logger.info(f"Found {len(components)} components for merging")
        for row in components:
            component = row[0]
            self.merge_component(component)
        _ = db.cypher_query("CALL gds.graph.drop('sameAsGraph')")

    def merge_component(self, component_id):
        source_nodes, target_node = get_nodes_for_component(component_id)
        if target_node is None: # nothing new to merge
            return None
        logger.info(f"Component {component_id}: Found {len(source_nodes)} to merge into {target_node.uri}")
        for source_node in source_nodes:
            self.merge_nodes(source_node, target_node)

    def merge_nodes(self,source_node_tmp, target_node_tmp, field_to_update="internalMergedSameAsHighToUri"):        
        source_node2 = Resource.self_or_ultimate_target_node(source_node_tmp) if not isinstance(source_node_tmp, Resource) else source_node_tmp
        source_node_tmp_uri = source_node_tmp.uri if isinstance(source_node_tmp, Resource) else source_node_tmp
        target_node2 = Resource.self_or_ultimate_target_node(target_node_tmp) if not isinstance(target_node_tmp, Resource) else target_node_tmp
        target_node_tmp_uri = target_node_tmp.uri if isinstance(target_node_tmp, Resource) else target_node_tmp
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
        recursively_merge_nodes(source_node, target_node, live_mode=True, field_to_update=field_to_update)
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

def get_nodes_for_component(component_id: int) -> Tuple[list[Organization], Union[None,Organization]]:
    query = f"""MATCH (n: Organization) 
                WHERE n.internalMergedSameAsHighToUri IS NULL 
                AND n.componentId = {component_id}
                AND NOT ANY(x in LABELS(n) WHERE x =~ ".+Activity")
                RETURN n
                ORDER BY n.internalDocId
                """
    org_rows, _ = db.cypher_query(query, resolve_objects=True)
    if len(org_rows) == 0:
        logger.info(f"Nothing new to merge for component {component_id}")
        return [], None
    target_org = org_rows[0][0]
    source_orgs = [x[0] for x in org_rows[1:]]
    return source_orgs, target_org

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

def recursively_merge_nodes(source_node, target_node, live_mode, field_to_update="internalMergedSameAsHighToUri"):
    if target_node is None:
        return None
    if target_node.internalMergedSameAsHighToUri is not None:
        # target node is already merged so don't keep collecting weights
        source_weights = weights_for_relationships(source_node.uri)
    else:
        source_weights = {}
    res = Resource.merge_node_connections(source_node, target_node, run_as_re_merge=False, live_mode=live_mode, field_to_update=field_to_update)
    if res is True:
        recursively_re_merge_node_via_same_as(target_node, run_as_re_merge=False, live_mode=live_mode, use_these_weights=source_weights)
    
def recursively_re_merge_node_via_same_as(source_node, live_mode=False, run_as_re_merge=True, use_these_weights={}):
    target_uri = source_node.internalMergedSameAsHighToUri
    if target_uri is None:
        return None
    target_node = Resource.get_by_uri(target_uri)
    if target_node is None:
        logger.warning(f"{source_node.uri} expected to merge to {target_uri} but node doesn't exist")
        return None
    res = Resource.merge_node_connections(source_node, target_node, run_as_re_merge=run_as_re_merge, 
                                          use_these_weights=use_these_weights, live_mode=live_mode)
    if res is True:
        recursively_re_merge_node_via_same_as(target_node,live_mode=live_mode,use_these_weights=use_these_weights)

def weights_for_relationships(node_uri):
    query = f"""MATCH (n: Resource)-[r]-(o: Resource) WHERE n.uri ='{node_uri}' 
                RETURN o.uri, r.weight"""
    vals, _ = db.cypher_query(query)
    source_weights = {x:y for x,y in vals}
    return source_weights
