from django.core.cache import cache
from topics.models import BasedInGeoMixin, ActivityMixin
from topics.model_queries import get_stats
import logging
from datetime import date, timedelta
from topics.geo_utils import get_geo_data
logger = logging.getLogger(__name__)

def rebuild_cache():
    nuke_cache()
    warm_up_cache()
    
def nuke_cache():
    cache.clear()

def warm_up_cache(max_date=date.today()):
    _, _, country_region_codes = get_geo_data()
    for country_region_code in country_region_codes.keys():
        logger.info(f"Warming up {country_region_code}")
        BasedInGeoMixin.based_in_country_region(country_region_code,allowed_to_set_cache=True)
        ActivityMixin.orgs_by_activity_where(country_region_code,allowed_to_set_cache=True)
    logger.info("Warming up stats")
    get_stats(max_date,allowed_to_set_cache=True)
