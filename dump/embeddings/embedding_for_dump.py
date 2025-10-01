import pickle
from integration.embedding_utils import setup, import_batch_json, create_new_embeddings, import_batch_vector
from neomodel import db
import os
import json
import logging
logger = logging.getLogger(__name__)

ORG_FNAME = "dump/embeddings/organization_industries.pickle"
IND_FNAME = "dump/embeddings/industry_cluster_representative_docs.pickle"
ABOUT_FNAME = "dump/embeddings/about_us_names.pickle"
IND_UPDATE_FNAME = "dump/embeddings/industry_sector_update.pickle"
IND_NEO4J_FNAME = "dump/embeddings/industry_cluster_neo4j.pickle" # For neo4j vector index
ORG_NEO4J_FNAME = "dump/embeddings/org_industry_neo4j.pickle"

def load_embeddings(fname):
    with open(fname, "rb") as f:
        data = pickle.load(f)
    return data

def update_embeddings(org_fname, ind_fname, about_fname, ind_update_fname, ind_cluster_neo4j_fname, org_neo4j_fname):
    org_inds = load_embeddings(org_fname)
    org_batch =  [{'uri':x[0], 'top_industry_names_embedding_json': json.loads(x[1])} for x in org_inds]
    ind_clus = load_embeddings(ind_fname)
    ind_batch = [{'uri':x[0], 'representative_doc_embedding_json': json.loads(x[1])} for x in ind_clus]
    about_us = load_embeddings(about_fname)
    about_batch = [{'uri':x[0], 'name_embedding_json':json.loads(x[1])} for x in about_us]
    ind_update = load_embeddings(ind_update_fname)
    ind_update_batch = [{'uri':x[0], 'industry_embedding_json':json.loads(x[1])} for x in ind_update]
    ind_neo4j = load_embeddings(ind_cluster_neo4j_fname)
    ind_neo4j_batch = [{'uri':x[0], 'representative_doc_embedding':x[1]} for x in ind_neo4j]
    org_neo4j_inds = load_embeddings(org_neo4j_fname)
    org_neo4j_batch =  [{'uri':x[0], 'industry_embedding': x[1]} for x in org_neo4j_inds]
    import_to_neo4j(org_batch, ind_batch, about_batch, ind_update_batch, ind_neo4j_batch, org_neo4j_batch)

def import_to_neo4j(org_batch, ind_batch, about_batch, ind_update_batch, ind_neo4j_batch, org_neo4j_batch):
    driver = setup()
    import_batch_json(driver, org_batch, 1, 'top_industry_names_embedding_json')
    import_batch_json(driver, ind_batch, 1, 'representative_doc_embedding_json' )
    import_batch_json(driver, about_batch, 1, 'name_embedding_json' )
    import_batch_json(driver, ind_update_batch, 1, 'industry_embedding_json')
    import_batch_vector(driver, ind_neo4j_batch, 1, 'representative_doc_embedding') 
    import_batch_vector(driver, org_neo4j_batch, 1, 'industry_embedding') 

def apply_latest_org_embeddings(force_recreate=False, org_fname=ORG_FNAME, ind_fname=IND_FNAME, 
                                about_fname=ABOUT_FNAME, ind_update_fname=IND_UPDATE_FNAME, ind_cluster_neo4j_fname=IND_NEO4J_FNAME,
                                org_neo4j_fname=ORG_NEO4J_FNAME):
    if force_recreate is False and (os.path.exists(org_fname) and os.path.exists(ind_fname)):
        logger.info("Loading embeddings from file")
        update_embeddings(org_fname, ind_fname, about_fname, ind_update_fname, ind_cluster_neo4j_fname, org_neo4j_fname)
    else:
        logger.info("Creating new embeddings")
        create_new_embeddings(really_run_me=True)
        save_latest_embeddings(org_fname, ind_fname, about_fname, ind_update_fname, ind_cluster_neo4j_fname, org_neo4j_fname)

def save_latest_embeddings(org_fname, ind_fname, about_fname, ind_update_fname, ind_cluster_neo4j_fname, org_neo4j_fname):
    org_query = "MATCH (n: Organization) WHERE n.top_industry_names_embedding_json IS NOT NULL RETURN n.uri, n.top_industry_names_embedding_json"
    org_embeddings, _ = db.cypher_query(org_query)
    with open(org_fname, "wb") as f:
        pickle.dump(org_embeddings,f)
    about_query = "MATCH (n: AboutUs) WHERE n.name_embedding_json IS NOT NULL RETURN n.uri, n.name_embedding_json"
    about_embeddings, _ = db.cypher_query(about_query)
    with open(about_fname, "wb") as f:
        pickle.dump(about_embeddings,f)
    ind_query = "MATCH (n: IndustryCluster) WHERE n.representative_doc_embedding_json IS NOT NULL RETURN n.uri, n.representative_doc_embedding_json"
    ind_embeddings, _ = db.cypher_query(ind_query)
    with open(ind_fname, "wb") as f:
        pickle.dump(ind_embeddings, f)
    ind_update_query = "MATCH (n: IndustrySectorUpdate) WHERE n.industry_embedding_json IS NOT NULL RETURN n.uri, n.industry_embedding_json"
    ind_update_embeddings, _ = db.cypher_query(ind_update_query)
    with open(ind_update_fname, "wb") as f:
        pickle.dump(ind_update_embeddings, f)
    ind_query_neo4j_query =  "MATCH (n: IndustryCluster) WHERE n.representative_doc_embedding IS NOT NULL RETURN n.uri, n.representative_doc_embedding"
    ind_embeddings_neo4j, _ = db.cypher_query(ind_query_neo4j_query)
    with open(ind_cluster_neo4j_fname, "wb") as f:
        pickle.dump(ind_embeddings_neo4j, f)
    org_neo4j_query = "MATCH (n: Organization) WHERE n.industry_embedding IS NOT NULL RETURN n.uri, n.industry_embedding"
    org_embeddings_neo4j, _ = db.cypher_query(org_neo4j_query)
    with open(org_neo4j_fname, "wb") as f:
        pickle.dump(org_embeddings_neo4j,f)
