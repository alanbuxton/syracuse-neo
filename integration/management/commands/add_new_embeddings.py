from django.core.management.base import BaseCommand
from integration.embedding_utils import create_new_embeddings
import logging
logger = logging.getLogger(__name__)

class Command(BaseCommand):

    def handle(self, *args, **options):
        create_new_embeddings(really_run_me=True)