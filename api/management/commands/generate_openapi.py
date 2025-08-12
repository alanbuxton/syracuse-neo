import json
from django.core.management.base import BaseCommand
from django.conf import settings
from drf_spectacular.generators import SchemaGenerator
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = "Generate .well-known/openapi.json from DRF Spectacular schema. MCP server can separately convert to mcp.json format"

    def handle(self, *args, **options):
        generator = SchemaGenerator()
        schema = generator.get_schema(request=None, public=True)

        output_path = settings.WELL_KNOWN_DIR / "openapi.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)

        logger.info(self.style.SUCCESS(f"Generated {output_path}"))