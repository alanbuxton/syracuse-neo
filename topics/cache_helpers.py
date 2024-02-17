from django.core.cache import cache
from topics.models import Organization, ActivityMixin, Person
from topics.model_queries import get_stats
import logging
from datetime import date, timedelta, datetime, timezone
from topics.geo_utils import get_geo_data
logger = logging.getLogger(__name__)

def is_cache_ready():
    ts = cache.get("cache_updated")
    if ts is None:
        return False
    return ts

def rebuild_cache():
    nuke_cache()
    warm_up_cache()

def nuke_cache():
    cache.clear()

def warm_up_cache(max_date=date.today()):
    _, _, country_region_codes = get_geo_data()
    for country_region_code in country_region_codes.keys():
        logger.info(f"Warming up {country_region_code}")
        Organization.by_country_region_industry(country_region_code,allowed_to_set_cache=True)
        Person.by_country_region_industry(country_region_code,allowed_to_set_cache=True)
        ActivityMixin.orgs_by_activity_where_industry(country_region_code,allowed_to_set_cache=True)
    logger.info("Warming up stats")
    get_stats(max_date,allowed_to_set_cache=True)
    cache.set("cache_updated",datetime.now(tz=timezone.utc))
