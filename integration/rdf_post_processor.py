from topics.models import Resource
from neomodel import db
import logging
from integration.neo4j_utils import output_same_as_stats, count_relationships, apoc_del_redundant_same_as
from datetime import datetime
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
        LIMIT 1
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

    def run_all_in_order(self):
        apoc_del_redundant_same_as()
        tsnow = datetime.utcnow().isoformat().replace(":","").replace(".","_")
        with open(f"merge_logs/merge_{tsnow}.log","w",encoding='utf-8') as f:
            write_log_header("add_document_extract_to_relationship",f)
            self.add_document_extract_to_relationship()
            write_log_header("merge_same_as_high_connections",f)
            self.merge_same_as_high_connections(f)

    def merge_same_as_high_connections(self,logfile=None):
        if logfile:
            logfunc = logfile.write
        else:
            logfunc = logger.debug
        cnt = 0
        log_count_relationships("Before merge_same_as_high_connections")
        while True:
            vals,_ = db.cypher_query(self.QUERY_SAME_AS_HIGH_FOR_MERGE,
                                        resolve_objects=True)
            if len(vals) == 0:
                break
            assert len(vals) == 1, f"{vals} should be one row"
            val_row = vals[0]
            target_node = val_row[0]
            source_node = val_row[1]
            logfunc(f"merging {source_node.uri} into {target_node.uri}\n")
            Resource.merge_node_connections(source_node,target_node)
            cnt += 1
            if cnt % 1000 == 0:
                log_count_relationships(f"After merge_same_as_high_connections {cnt} records")
            if cnt % 100 == 0:
                logger.info(f"Merged merge_same_as_high_connections {cnt} records")
        log_count_relationships("After merge_same_as_high_connections")


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

def write_log_header(message,logfile):
    logfile.writelines([
        "\n",
        "*" * 50,
        "\n",
        message,
        "\n",
        "*" * 50,
        "\n",
        ])

def log_count_relationships(text):
    cnt = count_relationships()
    logger.info(f"{text}: {cnt} relationships")
