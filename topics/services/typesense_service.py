import typesense
from django.conf import settings
import logging
from topics.management.commands.refresh_typesense import Command as RefreshTypesense
logger = logging.getLogger(__name__)

def reload_typesense():
    opts = {"force":True, "recreate_collection": True, "batch_size": 100, "dry_run": False}
    org_opts = opts | {"model_class": "topics.models.Organization"}
    ind_opts = opts | {"model_class": "topics.models.IndustryCluster"}
    RefreshTypesense().handle(**org_opts)
    RefreshTypesense().handle(**ind_opts) 

class TypesenseService:
    def __init__(self):
        self.client = typesense.Client(settings.TYPESENSE_CONFIG)
    
    def create_collections(self, collection_schemas: list[dict]):
        """Create Typesense collections for indexing"""     
        for collection_schema in collection_schemas:
            try:
                self.client.collections.create(collection_schema)
                logger.info(f"Created {collection_schema['name']} collection")
            except Exception as e:
                logger.warning(f"Couldn't create {collection_schema['name']} - might already exist: {e}")


    def search(self, name, collection_name, query_by="name",limit=100): 
        search_result = self.client.collections[collection_name].documents.search({
            'q': name,
            'query_by': query_by,
            'limit': limit,
        })
        results = search_result['hits']
        return results

    def vector_search_multi(self, query_vector, collection_names, query_field="embedding", limit=100):
        multi_search_queries = {
            'union': True, # Currently not used see https://github.com/typesense/typesense-python/pull/96#event-19083778984
            'searches': [
                self.build_query(query_vector, x, query_field, limit) for x in collection_names
            ]
        }
        return self.perform_multi_search(multi_search_queries)

    def build_query(self, query_vector, collection_name, query_field, limit):
        return  {
                    'collection': collection_name,
                    'q': '*',
                    'vector_query': f'{query_field}:([{",".join(map(str, query_vector))}], k:{limit})',
                    'exclude_fields': 'embedding'  # Don't return the vector in results
                }

    def vector_search(self, query_vector, collection_name, query_field="embedding", limit=100):
        # Use multi-search for vector-only queries
        multi_search_queries = {
            'searches': [
                self. build_query(query_vector, collection_name, query_field, limit)
            ]
        }
        return self.perform_multi_search(multi_search_queries)

    def perform_multi_search(self, multi_search_queries):
        res = self.client.multi_search.perform(multi_search_queries)
        matches = []
        for val in res['results']:
            if val['found'] > 0:
                matches.extend(val['hits'])
        return matches

    def search_by_uri(self, uri):
        searches = [
            {'collection': coll_name,
             'q': uri,
             'query_by': 'uri'}
            for coll_name in self.all_collection_names()
        ]
        multi_search_queries = {
            'union': True,
            'searches': searches
        }
        return self.perform_multi_search(multi_search_queries)

    def all_collection_names(self):
        colls = self.client.collections.retrieve()
        return [x['name'] for x in colls]