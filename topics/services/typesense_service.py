import typesense
from django.conf import settings
import typing
import logging
import time
from datetime import date
from topics.neo4j_utils import date_to_cypher_friendly
from syracuse.date_util import min_date_from_date
from neomodel import db
from requests.exceptions import ConnectionError

logger = logging.getLogger(__name__)

class TypesenseService(object):
    def __init__(self):
        self.client = typesense.Client(settings.TYPESENSE_CONFIG)

    def recreate_collection(self, model_class):
        collection_name = model_class.typesense_collection
        logger.info(f"Recreating collection '{collection_name}'...")
        try:
            self.client.collections[collection_name].delete()
            logger.info(f"Deleted existing collection '{collection_name}'")
        except Exception:
            pass  # Collection might not exist
        schema = model_class.typesense_schema()
        self.client.collections.create(schema)
        logger.info(f"Created collection '{collection_name}'")
    
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
    
    def doc_counts_by_collection(self, collection_name):
        try:
            collection_info = self.client.collections[collection_name].retrieve()
            doc_count = collection_info.get('num_documents', 0)
            logger.info(f"Collection {collection_name} now contains {doc_count} documents")
        except ConnectionError as e:
            logger.error(f"Can't connect typesense {e}")
            return None
        except Exception as e:
            logger.error(f"{collection_name} - Could not retrieve collection stats: {e}")
            raise
        return doc_count

    def delete_by_collection_and_doc_ids(self, collection_name, internal_doc_ids):
        doc_ids_string = ','.join(map(str,internal_doc_ids))
        try:
            self.client.collections[collection_name].documents.delete({
                'filter_by':f'internal_doc_id:[{doc_ids_string}]'
            })
        except Exception as e:
            logger.error(f"Could not delete {collection_name} {internal_doc_ids}")
            raise


def log_stats(client):
    try:
        metrics = client.metrics.retrieve()
        logger.debug(metrics)
        stats = client.api_call.get("/stats.json",entity_type=StatsResponse,as_json=True)
        logger.debug(stats)
    except ConnectionError as e:
        logger.error(f"Can't connect to typesense: {e}")
        metrics = {}
        stats = {}
    return metrics, stats

class StatsResponse(typing.TypedDict):
    pass


def delete_by_internal_doc_ids(collection_name, doc_ids):
    ts = TypesenseService()
    ts.delete_by_collection_and_doc_ids(collection_name, doc_ids)

def add_by_internal_doc_ids(classes_and_has_article, internal_doc_ids, max_date):
    if max_date is None:
        max_date = date.today()
    min_date, _ = min_date_from_date(max_date,days_diff=100) # cache is 90, so this will be more than enough
    for klass, has_article in classes_and_has_article:
        add_by_internal_doc_ids_and_class(klass, internal_doc_ids,
                                          has_article, min_date,
                                          )

def add_by_internal_doc_ids_and_class(model_class, internal_doc_ids: list[int],
                            has_article,
                            min_date,
                            batch_size = 200,
                            limit = 0,
                            sleep_time = 0):
    refresh_typesense_collection(model_class,batch_size,limit, sleep_time, 0, min_date, 
                                 has_article, doc_ids=internal_doc_ids,
                                 recreate_collection=False, save_metrics=False)

def get_next_batch(max_id, label, has_article, doc_ids, batch_size, min_date):
    query = f"""
            MATCH (n: Resource&{label})
            WHERE n.internalId > {max_id}
            AND n.internalMergedSameAsHighToUri IS NULL
            """
    conditions = ""

    if doc_ids:
        conditions = f""" AND n.internalDocId IN {list(doc_ids)} """
    elif has_article:
        conditions = f"""
            AND EXISTS {{
                MATCH (n)-[:documentSource]->(art:Article)
                WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
            }}
        """
    query = query + conditions
    query = query + f" RETURN n ORDER BY n.internalId LIMIT {batch_size}"
    logger.debug(query)
    results, _ = db.cypher_query(query)
    return results


def refresh_typesense_collection(model_class, batch_size, limit,
                                sleep_time, max_id, min_date, has_article,
                                doc_ids = None, recreate_collection = False,
                                save_metrics = False):
    
    ts = TypesenseService()
    client = ts.client

    if recreate_collection: # i.e. we want to load everything
        ts.recreate_collection(model_class)

    nodes_processed = 0
    total_docs = 0
    all_metrics = []
    all_stats = []

    label = model_class.__name__
    collection_name = model_class.typesense_collection

    while (limit == 0 or nodes_processed < limit):
        metrics, stats = log_stats(client)
        import_70p = stats.get("import_70Percentile_latency_ms",0)
        import_70p = float(import_70p)
        if import_70p > 50:
            logger.info(f"latency too high at {import_70p}, backing off")
            time.sleep(5)
            continue
        if save_metrics:
            do_save_metrics(all_metrics, all_stats, metrics, stats)

        batch_num = nodes_processed // batch_size + 1
        logger.info(f"Processing batch {batch_num}...")

        try:
            results = get_next_batch(max_id, label, has_article,
                                    doc_ids, batch_size, min_date)      
            if not results:
                logger.info("Didn't get any results, quitting")
                break

            # Convert to Typesense documents
            documents = []
            batch_processed = 0
            
            for row in results:
                node_data = row[0]
                node_instance = model_class.inflate(node_data)
                logger.debug(node_instance.uri)
                assert node_instance.internalId > max_id, f"{node_instance.internalId} should be higher than {max_id}"
                max_id = node_instance.internalId
                doc_or_docs = node_instance.to_typesense_doc()
                if isinstance(doc_or_docs, dict):
                    doc_or_docs = [doc_or_docs]
                for doc in doc_or_docs:
                    if doc:
                        documents.append(doc)
                batch_processed += 1
                logger.debug(documents)
            # Batch import to Typesense
            if documents:
                try:
                    results = client.collections[collection_name].documents.import_(
                        documents, {'action': 'upsert'}
                    )

                    # Check for any import errors
                    failed = []
                    for idx, row in enumerate(results):
                        if row.get('error'):
                            failed.append(f"row {idx} failed with {row['error']}")
                    if len(failed) > 0:
                        for x in failed:
                            logger.debug(f"failed: {x}")
                        raise ImportError(f"Failed to import {failed} documents in batch")

                except Exception as e:
                    logger.error(f"Error importing batch to Typesense: {e}")
                    raise
            
            nodes_processed += batch_processed
            total_docs += len(documents)
            av_docs_per_node = total_docs / nodes_processed
            logger.info(f"Processed {batch_processed} nodes ({len(documents)} docs). Max id {max_id} "
                        f"Total nodes processed {nodes_processed} with {total_docs} docs created, average per node: {av_docs_per_node}")
            time.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            raise

    logger.info(f"Completed processing {nodes_processed} nodes in collection '{collection_name}'")
    ts.doc_counts_by_collection(collection_name)


def do_save_metrics(all_metrics, all_stats, metrics, stats):
    all_metrics.append(metrics)
    all_stats.append(stats)
    with open("stats.pickle", "wb") as f:
        import pickle
        vals = {"metrics":all_metrics, "stats": all_stats}
        pickle.dump(vals, f)

