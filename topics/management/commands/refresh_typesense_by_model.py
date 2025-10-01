from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import typesense
import importlib
import logging
from neomodel import db
import time
from topics.services.typesense_service import log_stats
from topics.neo4j_utils import date_to_cypher_friendly
from syracuse.date_util import min_date_from_date
from datetime import date
from topics.models.models_extras import add_dynamic_classes_for_multiple_labels

logger = logging.getLogger(__name__)

class ImportError(Exception):
    pass

class Command(BaseCommand):
    help = 'Refresh all entities of a specified type in Typesense from neomodel database'

    def add_arguments(self, parser):
        parser.add_argument(
            'model_class',
            type=str,
            help='The model class to refresh (e.g., myapp.models.MyModel)'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=40, # default per https://typesense.org/docs/29.0/api/documents.html#index-multiple-documents
            help='Number of entities to process in each batch (default: 40)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Max docs to process (0 means no limit)'
        )
        parser.add_argument(
            '--sleep',
            type=int,
            default=0,
            help="Sleep time between batches"
        )
        parser.add_argument(
            '--id-starts-after',
            type=int,
            default=0,
            help="Will look for nodes with internalId higher than this"
        )
        parser.add_argument(
            '--save-metrics',
            default=False,
            action='store_true'
        )
        parser.add_argument(
            '--load_all',
            default=False,
            action='store_true',
            help="Load all data. If not set will load data related to last 90 days docs"
        )
        parser.add_argument(
            '--has-article',
            default=False,
            action='store_true',
            help="If true, then make sure there is a matching Article."
        )


    def handle(self, *args, **options):
        add_dynamic_classes_for_multiple_labels(ignore_cache=True)
        model_class_path = options['model_class']
        batch_size = options['batch_size']
        limit = options['limit']
        sleep_time = options['sleep']
        max_id = options['id_starts_after']
        save_metrics = options['save_metrics']
        load_all = options['load_all']
        has_article = options['has_article']

        if load_all:
            min_date = date(2000,1,1)
        else:
            min_date, _ = min_date_from_date(date.today())

        # Import the model class
        try:
            module_path, class_name = model_class_path.rsplit('.', 1)
            module = importlib.import_module(module_path)
            model_class = getattr(module, class_name)
        except (ValueError, ModuleNotFoundError, AttributeError) as e:
            raise CommandError(f"Could not import model class '{model_class_path}': {e}")

        # Verify it's a neomodel class
        if not hasattr(model_class, 'uri'):
            raise CommandError(f"'{model_class_path}' is not a valid neomodel class")

        label = model_class.__label__
        collection_name = model_class.typesense_collection
        
        # Initialize Typesense client
        try:
            client = self.get_typesense_client()
        except Exception as e:
            raise CommandError(f"Could not connect to Typesense: {e}")

        if has_article:
            count_query =f'''MATCH (n:Resource&{label})-[:documentSource]->(art: Resource&Article)
                    WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
                    AND n.internalMergedSameAsHighToUri IS NULL RETURN COUNT(DISTINCT(n))'''
        else:
            count_query = f'''MATCH (n:Resource&{label}) RETURN COUNT(n)'''

        result, _ = db.cypher_query(count_query)
        total_count = result[0][0] if result else 0

        if total_count == 0:
            logger.warning(f"No nodes found with label '{label}'")
            return

        logger.info(f"Found {total_count} nodes to refresh in Typesense")
        res = self.refresh_typesense_collection(
            client, model_class, collection_name, label, 
            batch_size, limit,
            sleep_time, max_id, min_date, has_article, save_metrics=save_metrics
        )

        return res

    def get_typesense_client(self):
        typesense_config = settings.TYPESENSE_CONFIG | {'connection_timeout_seconds':60}
        return typesense.Client(typesense_config)


    def refresh_typesense_collection(self, client, model_class, collection_name, 
                                   label, batch_size, limit,
                                   sleep_time, max_id, min_date, has_article,
                                   save_metrics = False):
        

        if max_id == 0: # i.e. we want to load everything
            logger.info(f"Recreating collection '{collection_name}'...")
            try:
                client.collections[collection_name].delete()
                logger.info(f"Deleted existing collection '{collection_name}'")
            except Exception:
                pass  # Collection might not exist
            
            schema = model_class.typesense_schema()
            client.collections.create(schema)
            logger.info(f"Created collection '{collection_name}'")

        nodes_processed = 0
        total_docs = 0
        all_metrics = []
        all_stats = []

        while (limit == 0 or nodes_processed < limit):
            if save_metrics:
                metrics, stats = log_stats(client)
                all_metrics.append(metrics)
                all_stats.append(stats)
                with open("stats.pickle", "wb") as f:
                    import pickle
                    vals = {"metrics":all_metrics, "stats": all_stats}
                    pickle.dump(vals, f)

                import_70p = stats.get("import_70Percentile_latency_ms",0)
                import_70p = float(import_70p)
                if import_70p > 30:
                    logger.info(f"latency too high at {import_70p}, backing off")
                    time.sleep(5)
                    continue
                
            batch_num = nodes_processed // batch_size + 1
            logger.info(f"Processing batch {batch_num}...")

            try:
                if has_article:
                    query = f"""
                    MATCH (n:Resource&{label})-[:documentSource]->(art: Resource&Article)
                    WHERE n.internalId > {max_id}
                    AND art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
                    AND n.internalMergedSameAsHighToUri IS NULL
                    """
                else:
                    query = f"""
                    MATCH (n:Resource&{label})
                    WHERE n.internalId > {max_id}
                    """

                query = query + f" RETURN DISTINCT(n) ORDER BY n.internalId LIMIT {batch_size}"
                
                results, _ = db.cypher_query(query)
                
                if not results:
                    logger.info("Didn't get any results, quitting")
                    break

                # Convert to Typesense documents
                documents = []
                batch_processed = 0
                
                for row in results:
                    node_data = row[0]
                    node_instance = model_class.inflate(node_data)
                    logger.info(node_instance.uri)
                    assert node_instance.internalId > max_id, f"{node_instance.internalId} should be higher than {max_id}"
                    max_id = node_instance.internalId
                    doc_or_docs = node_instance.to_typesense_doc()
                    if isinstance(doc_or_docs, dict):
                        doc_or_docs = [doc_or_docs]
                    for doc in doc_or_docs:
                        if doc:
                            documents.append(doc)
                    batch_processed += 1
                
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
                                logger.error(f"failed: {x}")
                            raise ImportError(f"Failed to import {failed} documents in batch")

                    except Exception as e:
                        logger.error(f"Error importing batch to Typesense: {e}")
                        raise
                
                nodes_processed += batch_processed
                total_docs += len(documents)
                av_docs_per_node = total_docs / nodes_processed
                logger.info(f"Processed {nodes_processed} nodes ({len(documents)} docs). Max id {max_id} "
                            f"Total docs created {total_docs}, average per node: {av_docs_per_node}")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                raise
        
        logger.info(f"Completed processing {nodes_processed} nodes in collection '{collection_name}'")

        # Show collection stats
        try:
            collection_info = client.collections[collection_name].retrieve()
            doc_count = collection_info.get('num_documents', 0)
            logger.info(f"Collection now contains {doc_count} documents")
        except Exception as e:
            logger.error(f"Could not retrieve collection stats: {e}")
            raise
