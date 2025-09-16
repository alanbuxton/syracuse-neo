# Based on https://neo4j.com/docs/genai/tutorials/embeddings-vector-indexes/embeddings/sentence-transformers/

import neo4j
from neomodel import db
from sentence_transformers import SentenceTransformer
from syracuse.settings import (NEOMODEL_NEO4J_SCHEME,
    NEOMODEL_NEO4J_USERNAME,NEOMODEL_NEO4J_PASSWORD,
    NEOMODEL_NEO4J_HOSTNAME,NEOMODEL_NEO4J_PORT,
    EMBEDDINGS_MODEL, CREATE_NEW_EMBEDDINGS)
import logging
from topics.services.typesense_service import TypesenseService
import re

logger = logging.getLogger(__name__)

NEW_ABOUT_US_QUERY = '''MATCH (n:Resource&AboutUs)
WHERE n.name_embedding_json IS NULL
AND n.name IS NOT NULL
RETURN n.uri as uri, n.name as name'''

NEW_INDUSTRY_SECTOR_UPDATE_QUERY = '''MATCH (n:Resource&IndustrySectorUpdate)
WHERE n.industry_embedding_json IS NULL
AND n.industry IS NOT NULL
RETURN n.uri as uri, n.industry as industry'''

NEW_ORGANIZATION_INDUSTRY_QUERY = '''MATCH (n:Resource&Organization)
WHERE n.industry_embedding_json IS NULL
AND n.industry IS NOT NULL RETURN n.uri as uri, n.industry as industry'''

NEW_INDUSTRY_REPRESENTATIVE_DOCS_QUERY = '''MATCH (n:Resource&IndustryCluster)
WHERE n.representative_doc_embedding_json IS NULL
AND n.representativeDoc IS NOT NULL
return n.uri as uri, n.representativeDoc as representative_doc'''

# ORGANIZATION_SEARCH_BY_INDUSTRY_QUERY = ''' CALL db.index.vector.queryNodes('organization_industries_vec', 5, $queryEmbedding)
#     YIELD node, score
#     RETURN node, score
#     '''

# INDUSTRY_SEARCH_QUERY = ''' CALL db.index.vector.queryNodes('industry_cluster_representative_docs_vec', 5, $queryEmbedding)
#     YIELD node, score
#     RETURN node, score
#     '''

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
    create_entity_embeddings(driver,model, NEW_INDUSTRY_REPRESENTATIVE_DOCS_QUERY, 'representative_doc')
    create_entity_embeddings(driver,model, NEW_ORGANIZATION_INDUSTRY_QUERY, 'industry', min_words=1)
    create_entity_embeddings(driver, model, NEW_ABOUT_US_QUERY, 'name' )
    create_entity_embeddings(driver, model, NEW_INDUSTRY_SECTOR_UPDATE_QUERY, 'industry', min_words=1)

def setup(uri=URI, auth=AUTH):
    driver = neo4j.GraphDatabase.driver(uri, auth=auth)
    driver.verify_connectivity()
    return driver

def create_entity_embeddings(driver, model, query, fieldname, limit_per_query=10000, min_words=2):
    batch_size = 100
    batch_n = 1
    batch_for_update = []
    embedding_field = f'{fieldname}_embedding_json'
    query_with_limit = f"{query} LIMIT {limit_per_query}"
    logger.info(f"create_entity_embeddings with {fieldname} and query {query_with_limit[:100]}")
    got_records = False
    with driver.session(database=DB_NAME) as session:
        result = session.run(query_with_limit)
        for record in result:
            got_records = True
            source_vals = record.get(fieldname) 
            embeddable_vals = [x for x in source_vals if len(x.split()) >= min_words]
            embedding = model.encode(embeddable_vals)
            batch_for_update.append(
                {'uri':record.get('uri'), embedding_field: embedding}
            )  
            if len(batch_for_update) == batch_size:
                batch_n = import_batch_json(driver, batch_for_update, batch_n, embedding_field)
                batch_for_update = []
        import_batch_json(driver, batch_for_update, batch_n, embedding_field)
    if got_records:
        create_entity_embeddings(driver, model, query, fieldname, limit_per_query)
    

def import_batch_json(driver, nodes_with_embeddings, batch_n, field):
    driver.execute_query(f'''
    UNWIND $nodes AS node
    MATCH (n:Resource {{uri: node.uri}})
    SET n.{field} = apoc.convert.toJson(node.{field})                 
    ''', nodes=nodes_with_embeddings)
    logger.info(f'Processed batch {batch_n}.')
    return batch_n + 1

def do_vector_search(text, base_query, model=MODEL):
    query_embedding = model.encode(text)
    assert "$query_embedding" in base_query, f"Expected {base_query} to include $query_embedding"
    res, _ = db.cypher_query(base_query, params={'query_embedding':query_embedding}, resolve_objects=True)
    return res

def create_embeddings_for_strings(strings: list[str], model=MODEL):
    if strings is None:
        return []
    return [x.tolist() for x in model.encode(strings)]

def do_vector_search_typesense(text: str, collection_name: str, model=MODEL, limit=100):
    query_embedding = model.encode(text)
    ts = TypesenseService()
    ts_vals = ts.vector_search(query_embedding, collection_name=collection_name, limit=limit) or []
    return ts_vals

def do_vector_search_typesense_multi_collection(text: str, collection_names: list, model=MODEL, limit=100):
    query_embedding = model.encode(text)
    ts = TypesenseService()
    ts_vals = ts.vector_search_multi(query_embedding, collection_names=collection_names, limit=limit) or []
    return ts_vals

def create_industry_cluster_representative_doc_embeddings(driver, model):
    batch_size = 10
    batch_n = 1
    batch_for_update = []
    logger.info("Starting update_industry_cluster_embeddings")
    query = '''MATCH (n:Resource&IndustryCluster)
        WHERE n.representative_doc_embedding IS NULL
        AND n.representativeDoc IS NOT NULL
        return n.uri as uri, n.representativeDoc as representative_doc'''
    with driver.session(database=DB_NAME) as session:
        result = session.run(query)
        for record in result:
            representative_docs = record.get("representative_doc")
            uri = record.get("uri")
            representative_docs = [re.sub( re.compile(r"industry",re.IGNORECASE), "", x) for x in representative_docs]
            for_embedding = " and ".join(sorted(representative_docs,key=len)[:2]).lower()
            logger.debug(f"Working on uri {uri} representative_doc {representative_docs} ({for_embedding})")
            embedding = model.encode(for_embedding)
            batch_for_update.append(
                {'uri':uri, 'representative_doc_embedding': embedding}
            )
            if len(batch_for_update) == batch_size:
                batch_n = import_batch_vector(driver, batch_for_update, batch_n, 'representative_doc_embedding')
                batch_for_update = []
        import_batch_vector(driver, batch_for_update, batch_n, 'representative_doc_embedding')

def create_organization_industry_embeddings(driver, model):
    batch_size = 100
    batch_n = 1
    batch_for_update = []
    logger.info("Starting update_organization_industry_embeddings")
    query = '''MATCH (n:Resource&Organization)
                WHERE n.industry_embedding IS NULL
                AND n.industry IS NOT NULL RETURN n.uri as uri, n.industry as industry
                '''
    with driver.session(database=DB_NAME) as session:
        result = session.run(query)
        for record in result:
            embedding = model.encode("; ".join(record.get('industry')))
            batch_for_update.append(
                {'uri':record.get('uri'), 'industry_embedding': embedding}
            )
            if len(batch_for_update) == batch_size:
                batch_n = import_batch_vector(driver, batch_for_update, batch_n, 'industry_embedding')
                batch_for_update = []
        import_batch_vector(driver, batch_for_update, batch_n, 'industry_embedding')

def import_batch_vector(driver, nodes_with_embeddings, batch_n, field):
    driver.execute_query(f'''
    UNWIND $nodes AS node
    MATCH (n:Resource {{uri: node.uri}})
    CALL db.create.setNodeVectorProperty(n, '{field}', node.{field})
    ''', nodes=nodes_with_embeddings)
    logger.info(f'Processed batch {batch_n}.')
    return batch_n + 1
