
from sentence_transformers import SentenceTransformer
from syracuse.settings import EMBEDDINGS_MODEL
import logging
from topics.services.typesense_service import TypesenseService
from neomodel import db

MODEL=SentenceTransformer(EMBEDDINGS_MODEL)

logger = logging.getLogger(__name__)

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


def do_vector_search(text, base_query, model=MODEL):
    query_embedding = model.encode(text)
    assert "$query_embedding" in base_query, f"Expected {base_query} to include $query_embedding"
    res, _ = db.cypher_query(base_query, params={'query_embedding':query_embedding}, resolve_objects=True)
    return res
