from django.core.management.base import BaseCommand
from topics.services.typesense_service import TypesenseService
from topics.models import Organization, IndustryCluster, AboutUs, IndustrySectorUpdate

import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Setup Typesense collections'
    
    def handle(self, *args, **options):
        service = TypesenseService()
        service.create_collections([Organization.typesense_schema(), IndustryCluster.typesense_schema(),
                                    AboutUs.typesense_schema(),IndustrySectorUpdate.typesense_schema(),
                                    ])
        logger.info("Finished Setup Typesense collections")
