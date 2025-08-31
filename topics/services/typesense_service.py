import typesense
from django.conf import settings
import logging
logger = logging.getLogger(__name__)

class TypesenseService:
    def __init__(self):
        self.client = typesense.Client(settings.TYPESENSE_CONFIG)
    
    def create_collections(self, collection_names: list[str]):
        """Create Typesense collections for indexing"""     
        for collection_name in collection_names:
            try:
                self.client.collections.create(collection_name)
                logger.info(f"Created {collection_name} collection")
            except Exception as e:
                logger.warning(f"Couldn't create {collection_name} - might already exist: {e}")


    def search(self, name, collection_name, query_by="name"): 
        search_result = self.client.collections[collection_name].documents.search({
            'q': name,
            'query_by': query_by,
            'limit': 10,
        })
        results = search_result['hits']
        return results


    def vector_search(self, query_vector, collection_name, query_field="embedding", limit=100):
        # Use multi-search for vector-only queries
        multi_search_queries = {
            'searches': [
                {
                    'collection': collection_name,
                    'q': '*',
                    'vector_query': f'{query_field}:([{",".join(map(str, query_vector))}], k:{limit})',
                    'exclude_fields': 'embedding'  # Don't return the vector in results
                }
            ]
        }
        res = self.client.multi_search.perform(multi_search_queries)
        results = res['results']
        return results[0]['hits']