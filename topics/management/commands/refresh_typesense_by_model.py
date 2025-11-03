from django.core.management.base import BaseCommand, CommandError
import importlib
import logging
from neomodel import db
from topics.services.typesense_service import refresh_typesense_collection
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
            '--load-all',
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

        if has_article:
            count_query =f'''MATCH (n:Resource&{label})
                    WHERE n.internalMergedSameAsHighToUri IS NULL
                    AND EXISTS {{
                        MATCH (n)-[:documentSource]->(art: Resource&Article)
                        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
                    }}
                    RETURN COUNT(n)'''
        else:
            count_query = f'''MATCH (n:Resource&{label}) RETURN COUNT(n)'''

        result, _ = db.cypher_query(count_query)
        total_count = result[0][0] if result else 0

        if total_count == 0:
            logger.warning(f"No nodes found with label '{label}'")
            return

        recreate_collection = True if max_id == 0 else False
        logger.info(f"Found {total_count} nodes to refresh in Typesense")
        res = refresh_typesense_collection(
            model_class, 
            batch_size, limit,
            sleep_time, max_id, min_date, has_article, doc_ids=None,
            recreate_collection=recreate_collection, save_metrics=save_metrics
        )

        return res


