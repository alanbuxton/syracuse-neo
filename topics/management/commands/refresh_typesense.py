from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
import typesense
import importlib
import logging
from neomodel import db
import time

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
            '--recreate-collection',
            action='store_true',
            help='Drop and recreate the Typesense collection'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be refreshed without actually doing it'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Skip confirmation prompt'
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


    def handle(self, *args, **options):
        model_class_path = options['model_class']
        batch_size = options['batch_size']
        recreate_collection = options['recreate_collection']
        dry_run = options['dry_run']
        force = options['force']
        limit = options['limit']
        sleep_time = options['sleep']

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

        if dry_run:
            logger.info(f"DRY RUN: Would refresh collection '{collection_name}' with '{label}' nodes")

        # Get total count from Neo4j
        try:
            count_query = f"MATCH (n:{label}) RETURN count(n) as total"
            result, _ = db.cypher_query(count_query)
            total_count = result[0][0] if result else 0
        except Exception as e:
            raise CommandError(f"Error counting nodes: {e}")

        if total_count == 0:
            logger.warning(f"No nodes found with label '{label}'")
            return

        logger.info(f"Found {total_count} nodes to refresh in Typesense")

        # Confirmation prompt
        if not force and not dry_run:
            action = "recreate and populate" if recreate_collection else "refresh"
            confirm = input(f"Are you sure you want to {action} collection '{collection_name}' with {total_count} documents? (y/N): ")
            if confirm.lower() != 'y':
                logger.info("Operation cancelled.")
                return

        if dry_run:
            logger.info(f"DRY RUN complete. Would have refreshed {total_count} documents.")
            return

        if limit == 0:
            max_items = total_count
        else:
            max_items = limit if total_count > limit else total_count

        # Execute the refresh
        self.refresh_typesense_collection(
            client, model_class, collection_name, label, 
            batch_size, max_items, recreate_collection,
            sleep_time
        )

    def get_typesense_client(self):
        """Initialize Typesense client from Django settings"""
        # Adjust these settings based on your configuration
        typesense_config = settings.TYPESENSE_CONFIG | {'connection_timeout_seconds':60}
        return typesense.Client(typesense_config)


    def refresh_typesense_collection(self, client, model_class, collection_name, 
                                   label, batch_size, total_count, recreate_collection,
                                   sleep_time):
        """Main refresh logic"""
        
        # Handle collection creation/recreation
        if recreate_collection:
            logger.info(f"Recreating collection '{collection_name}'...")
            try:
                client.collections[collection_name].delete()
                logger.info(f"Deleted existing collection '{collection_name}'")
            except Exception:
                pass  # Collection might not exist
            
            schema = model_class.typesense_schema()
            client.collections.create(schema)
            logger.info(f"Created collection '{collection_name}'")
        else:
            # Clear existing documents
            try:
                client.collections[collection_name].documents.delete({'filter_by': '*'})
                logger.info(f"Cleared existing documents from '{collection_name}'")
            except Exception as e:
                logger.error(f"Could not clear collection: {e}")
                raise

        # Process in batches
        processed = 0
        
        while processed < total_count:
            batch_num = processed // batch_size + 1
            logger.info(f"Processing batch {batch_num}...")
            
            skip = processed
            limit = min(batch_size, total_count - processed)
            
            try:
                # Fetch batch from Neo4j
                query = f"""
                MATCH (n:{label})
                RETURN n
                SKIP {skip}
                LIMIT {limit}
                """
                
                results, _ = db.cypher_query(query)
                
                if not results:
                    break
                
                # Convert to Typesense documents
                documents = []
                batch_processed = 0
                
                for row in results:
                    try:
                        node_data = row[0]
                        node_instance = model_class.inflate(node_data)
                        doc_or_docs = node_instance.to_typesense_doc()
                        if isinstance(doc_or_docs, dict):
                            documents.append(doc_or_docs)
                        else:
                            documents.extend(doc_or_docs)
                        batch_processed += 1
                    except Exception as e:
                        logger.error(f"Error converting node to document: {e}")
                        continue
                
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
                
                processed += batch_processed
                logger.info(f"Processed {processed}/{total_count} documents")
                time.sleep(sleep_time)
                
            except Exception as e:
                logger.error(f"Error processing batch: {e}")
                raise
        
        logger.info(f"Successfully refreshed {processed} documents in collection '{collection_name}'")

        # Show collection stats
        try:
            collection_info = client.collections[collection_name].retrieve()
            doc_count = collection_info.get('num_documents', 0)
            logger.info(f"Collection now contains {doc_count} documents")
        except Exception as e:
            logger.error(f"Could not retrieve collection stats: {e}")
            raise