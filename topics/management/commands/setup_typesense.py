from django.core.management.base import BaseCommand
from topics.services.typesense_service import TypesenseService
from topics.models import Organization, IndustryCluster, AboutUs, IndustrySectorUpdate

import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Setup Typesense collections'

    def handle(self, *args, **options):
        service = TypesenseService()
        schemas = [Organization.typesense_schema(), IndustryCluster.typesense_schema(),
                                    AboutUs.typesense_schema(),IndustrySectorUpdate.typesense_schema(),
                                    ]
        service.create_collections(schemas)
        logger.info("Finished Setup Typesense collections")
