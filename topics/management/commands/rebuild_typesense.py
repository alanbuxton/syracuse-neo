from django.core.management.base import BaseCommand
from topics.management.commands.refresh_typesense_by_model import Command as RefreshTypesenseByModel
import logging
logger = logging.getLogger(__name__)

def refresh_all(limit=0):
    base_opts = {"batch_size":200,"limit":limit,"sleep":0,"id_starts_after":0,"save_metrics":True,"has_article":True,"load_all":False}
    model_opts = [
        {"model_class": "topics.models.IndustrySectorUpdate", "load_all": True }, # Need all of these as they have related orgs
        {"model_class": "topics.models.Organization"},
        {"model_class": "topics.models.AboutUs"},
        {"model_class": "topics.models.IndustryCluster", "has_article": False},
    ]
    for model_class_opts in model_opts:
        opts = base_opts | model_class_opts
        logger.info(f"**** RECREATING {model_class_opts['model_class']} with limit {limit} ****")
        RefreshTypesenseByModel().handle( **opts)

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Max docs to process (0 means no limit)'
        )

    def handle(self, *args, **options):
        refresh_all(options['limit'])