# Based on https://neo4j.com/docs/genai/tutorials/embeddings-vector-indexes/embeddings/sentence-transformers/

import neo4j
from neomodel import db
from sentence_transformers import SentenceTransformer
from syracuse.settings import (NEOMODEL_NEO4J_SCHEME,
    NEOMODEL_NEO4J_USERNAME,NEOMODEL_NEO4J_PASSWORD,
    NEOMODEL_NEO4J_HOSTNAME,NEOMODEL_NEO4J_PORT,
    EMBEDDINGS_MODEL, CREATE_NEW_EMBEDDINGS)
import logging
logger = logging.getLogger(__name__)

NEW_ORGANIZATION_INDUSTRY_QUERY = '''MATCH (n:Resource&Organization)
WHERE n.industry_embedding IS NULL
AND n.industry IS NOT NULL RETURN n.uri as uri, n.industry as industry'''

NEW_INDUSTRY_REPRESENTATIVE_DOCS_QUERY = '''MATCH (n:Resource&IndustryCluster)
WHERE n.representative_doc_embedding IS NULL
AND n.representativeDoc IS NOT NULL
return n.uri as uri, n.representativeDoc as representative_doc'''

ORGANIZATION_SEARCH_BY_INDUSTRY_QUERY = ''' CALL db.index.vector.queryNodes('organization_industries_vec', 5, $queryEmbedding)
    YIELD node, score
    RETURN node, score
    '''

INDUSTRY_SEARCH_QUERY = ''' CALL db.index.vector.queryNodes('industry_cluster_representative_docs_vec', 5, $queryEmbedding)
    YIELD node, score
    RETURN node, score
    '''

URI=f"{NEOMODEL_NEO4J_SCHEME}://{NEOMODEL_NEO4J_HOSTNAME}:{NEOMODEL_NEO4J_PORT}"
AUTH=(NEOMODEL_NEO4J_USERNAME,NEOMODEL_NEO4J_PASSWORD)
DB_NAME="neo4j"

MODEL=SentenceTransformer(EMBEDDINGS_MODEL)

def create_new_embeddings(uri=URI, auth=AUTH, model=MODEL, really_run_me=CREATE_NEW_EMBEDDINGS):
    if really_run_me is not True:
        logger.info("Not running embeddings - check env var CREATE_NEW_EMBEDDINGS")
        return None
    driver = setup(uri, auth)
    create_industry_cluster_representative_doc_embeddings(driver,model)
    create_organization_industry_embeddings(driver,model)

def setup(uri=URI, auth=AUTH):
    driver = neo4j.GraphDatabase.driver(uri, auth=auth)
    driver.verify_connectivity()
    return driver

def create_organization_industry_embeddings(driver, model):
    batch_size = 100
    batch_n = 1
    batch_for_update = []
    logger.info("Starting update_organization_industry_embeddings")
    with driver.session(database=DB_NAME) as session:
        result = session.run(NEW_ORGANIZATION_INDUSTRY_QUERY)
        for record in result:
            embedding = model.encode("; ".join(record.get('industry')))
            batch_for_update.append(
                {'uri':record.get('uri'), 'industry_embedding': embedding}
            )
            # Import when a batch of movies has embeddings ready; flush buffer
            if len(batch_for_update) == batch_size:
                batch_n = import_batch(driver, batch_for_update, batch_n, 'industry_embedding')
                batch_for_update = []
        import_batch(driver, batch_for_update, batch_n, 'industry_embedding')

def create_industry_cluster_representative_doc_embeddings(driver, model):
    batch_size = 10
    batch_n = 1
    batch_for_update = []
    logger.info("Starting update_organization_industry_embeddings")
    with driver.session(database=DB_NAME) as session:
        result = session.run(NEW_INDUSTRY_REPRESENTATIVE_DOCS_QUERY)
        for record in result:
            representative_docs = record.get("representative_doc")
            uri = record.get("uri")
            joined = "; ".join(sorted(representative_docs,key=len))
            logger.info(f"Working on {result} - uri {uri} representative_doc {representative_docs} ({joined})")
            embedding = model.encode(joined)
            batch_for_update.append(
                {'uri':uri, 'representative_doc_embedding': embedding}
            )
            # Import when a batch of movies has embeddings ready; flush buffer
            if len(batch_for_update) == batch_size:
                batch_n = import_batch(driver, batch_for_update, batch_n, 'representative_doc_embedding')
                batch_for_update = []
        import_batch(driver, batch_for_update, batch_n, 'representative_doc_embedding')


def import_batch(driver, nodes_with_embeddings, batch_n, field):
    driver.execute_query(f'''
    UNWIND $nodes AS node
    MATCH (n:Resource {{uri: node.uri}})
    CALL db.create.setNodeVectorProperty(n, '{field}', node.{field})
    ''', nodes=nodes_with_embeddings)
    logger.info(f'Processed batch {batch_n}.')
    return batch_n + 1

def do_vector_search(text, base_query, model=MODEL):
    query_embedding = model.encode(text)
    assert "$query_embedding" in base_query, f"Expected {base_query} to include $query_embedding"
    res, _ = db.cypher_query(base_query, params={'query_embedding':query_embedding}, resolve_objects=True)
    return res
