from django.core.management.base import BaseCommand
from integration.rdf_post_processor import re_merge_all_orgs, add_dynamic_classes_for_multiple_labels
from integration.neo4j_utils import rerun_all_redundant_same_as
import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("-l","--live_mode",
                default=False,
                action="store_true",
                )
        parser.add_argument("-s","--rerun_all_same_as",
                            default=False,
                            action="store_true")

    def handle(self, *args, **options):
        live_mode = options['live_mode']
        logger.info(f"{self.__class__.__name__} running in live_mode? {live_mode}")
        add_dynamic_classes_for_multiple_labels(ignore_cache=True)
        re_merge_all_orgs(live_mode=options["live_mode"])

        rerun_same_as = options['rerun_all_same_as']
        if rerun_same_as is True:
            logger.info("Re-running all sameAsHigh deletions")
            rerun_all_redundant_same_as()



        


