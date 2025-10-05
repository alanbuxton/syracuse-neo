import typesense
from django.conf import settings
import typing
import logging
logger = logging.getLogger(__name__)

class TypesenseService(object):
    def __init__(self):
        self.client = typesense.Client(settings.TYPESENSE_CONFIG)
    
    def create_collections(self, collection_schema_names: list[dict]):
        for schema_name in collection_schema_names:
            try:
                self.client.collections.create(schema_name)
                logger.info(f"Created {schema_name} collection")
            except Exception as e:
                logger.warning(f"Couldn't create {schema_name} - might already exist: {e}")

    def list_collections(self):
        colls = self.client.collections.retrieve()
        collection_names = [x['name'] for x in colls]
        return collection_names

    def search(self, name, collection_name, query_by="name",limit=100): 
        search_result = self.client.collections[collection_name].documents.search({
            'q': name,
            'query_by': query_by,
            'limit': limit,
        })
        results = search_result['hits']
        return results

    def vector_search_multi(self, query_vector, collection_names, query_field="embedding", regions=None, limit=100):
        if regions:
            filter_fields = {"name": "region_list", "vals": regions}
        else:
            filter_fields = {}
        multi_search_queries = {
            'union': True, # Currently not used see https://github.com/typesense/typesense-python/pull/96#event-19083778984
            'searches': [
                self.build_query(query_vector, x, query_field, filter_fields, limit) for x in collection_names
            ]
        }
        return self.perform_multi_search(multi_search_queries)

    def build_query(self, query_vector, collection_name, query_field, filter_fields, limit):
        '''
            filter_fields: {"name": <field_name>, "vals": [list of vals]}
        '''
        query =  {
                    'collection': collection_name,
                    'q': '*',
                    'vector_query': f'{query_field}:([{",".join(map(str, query_vector))}], k:{limit})',
                    'exclude_fields': 'embedding'  # Don't return the vector in results
                }
        if filter_fields and self.has_field(collection_name, filter_fields['name']):
            filter_fields = f'{filter_fields["name"]}:=[{",".join(filter_fields["vals"])}]' 
            query['filter_by'] = filter_fields
        logger.debug(query)
        return query

    def vector_search(self, query_vector, collection_name, query_field="embedding", filter_fields = {}, limit=100):
        # Use multi-search for vector-only queries
        multi_search_queries = {
            'searches': [
                self.build_query(query_vector, collection_name, query_field, filter_fields, limit)
            ]
        }
        return self.perform_multi_search(multi_search_queries)

    def perform_multi_search(self, multi_search_queries):
        res = self.client.multi_search.perform(multi_search_queries)
        matches = []
        for val in res['results']:
            logger.debug(val)
            foundval = val.get('found')
            if foundval is None:
                raise ValueError(f"Error {val} when parsing {multi_search_queries}")
            if foundval > 0:
                for hit in val['hits']:
                    hit_d = dict(hit)
                    hit_d['collection_name'] = val['request_params']['collection_name']
                    matches.append(hit_d)
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
    
    def iterate_documents_generator(self, collection_name):
        '''
        e.g. use like this:
            ts = TypesenseService()
            max_docs = 10
            docs = []
            for doc in ts.iterate_documents_generator("about_us"):
                if doc.get("org_internal_ids",None):
                    docs.append(doc)
                if len(docs) > max_docs:
                    break
        '''
        export_data = self.client.collections[collection_name].documents.export()
        for line in export_data.strip().split('\n'):
            if line.strip():
                import json
                yield json.loads(line)


    def has_field(self, collection_name, field_name):
        schema = self.client.collections[collection_name].retrieve()
        return any(field['name'] == field_name for field in schema['fields'])

def log_stats(client):
    metrics = client.metrics.retrieve()
    logger.info(metrics)
    stats = client.api_call.get("/stats.json",entity_type=StatsResponse,as_json=True)
    logger.info(stats)
    return metrics, stats



class StatsResponse(typing.TypedDict):
    pass

