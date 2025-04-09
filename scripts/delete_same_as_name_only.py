from neomodel import *
import logging
logger = logging.getLogger(__name__)

def delete_same_as_name_only(live_mode=False):
    query = "MATCH (n: Resource)-[x:sameAsNameOnly]-() RETURN x"
    action = "DELETE x"
    apoc_query = f'CALL apoc.periodic.iterate("{query}","{action}",{{}})'
    if live_mode is True:
        db.cypher_query(apoc_query)


def relationship_stats():
    query = """CALL db.relationshipTypes() YIELD relationshipType as type
        CALL apoc.cypher.run('MATCH ()-[:`'+type+'`]->() RETURN count(*) as count',{}) YIELD value
        RETURN type, value.count
    """
    vals, _ = db.cypher_query(query)
    logger.info(vals)

def run_delete_same_as_name_only(live_mode=False):
    logger.info("**** BEFORE ****")
    relationship_stats()
    delete_same_as_name_only(live_mode)
    logger.info("**** AFTER ****")
    relationship_stats()
