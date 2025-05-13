import pickle
from integration.vector_search_utils import setup, import_batch, create_new_embeddings
from neomodel import db
import os
import logging
logger = logging.getLogger(__name__)

ORG_FNAME = "dump/embeddings/organization_industries.pickle"
IND_FNAME = "dump/embeddings/industry_cluster_representative_docs.pickle"

def load_organization_industries(fname):
    with open(fname, "rb") as f:
        org_industries = pickle.load(f)
    return org_industries

def load_industry_cluster_representative_docs(fname):
    with open(fname, "rb") as f:
        ind_repr_docs = pickle.load(f)
    return ind_repr_docs

def update_embeddings(org_fname, ind_fname):
    org_inds = load_organization_industries(org_fname)
    org_batch =  [{'uri':x[0], 'industry_embedding': x[1]} for x in org_inds]
    ind_clus = load_industry_cluster_representative_docs(ind_fname)
    ind_batch = [{'uri':x[0], 'representative_doc_embedding':x[1]} for x in ind_clus]
    import_to_neo4j(org_batch, ind_batch)

def import_to_neo4j(org_batch, ind_batch):
    driver = setup()
    import_batch(driver, org_batch, 1, 'industry_embedding')
    import_batch(driver, ind_batch, 2, 'representative_doc_embedding' )

def org_embeddings_exist():
    # Assume checking the org embeddings is good enough: if those don't exist then recreate
    org_query = "MATCH (n: Organization) WHERE n.industry_embedding IS NOT NULL RETURN n.uri, n.industry_embedding"
    orgs, _ = db.cypher_query(org_query)
    return len(orgs) > 0

def ind_embeddings_exist():
    ind_query = "MATCH (n: IndustryCluster) WHERE n.representative_doc_embedding IS NOT NULL RETURN n.uri, n.representative_doc_embedding"
    inds, _ = db.cypher_query(ind_query)
    return len(inds) > 0

def apply_latest_org_embeddings(force_recreate=False,org_fname=ORG_FNAME, ind_fname=IND_FNAME):
    if force_recreate is False and (os.path.exists(org_fname) and os.path.exists(ind_fname)):
        logger.info("Loading embeddings from file")
        update_embeddings(org_fname, ind_fname)
    else:
        logger.info("Creating new embeddings")
        create_new_embeddings(really_run_me=True)
        save_latest_embeddings(org_fname, ind_fname)

def save_latest_embeddings(org_fname=ORG_FNAME, ind_fname=IND_FNAME):
    org_query = "MATCH (n: Organization) WHERE n.industry_embedding IS NOT NULL RETURN n.uri, n.industry_embedding"
    org_embeddings, _ = db.cypher_query(org_query)
    with open(org_fname, "wb") as f:
        pickle.dump(org_embeddings,f)
    ind_query = "MATCH (n: IndustryCluster) WHERE n.representative_doc_embedding IS NOT NULL RETURN n.uri, n.representative_doc_embedding"
    ind_embeddings, _ = db.cypher_query(ind_query)
    with open(ind_fname, "wb") as f:
        pickle.dump(ind_embeddings, f)

