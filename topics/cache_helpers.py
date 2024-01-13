from django.core.cache import cache
from topics.models import BasedInGeoMixin, ActivityMixin
from topics.model_queries import get_stats
import logging
from datetime import date, timedelta
logger = logging.getLogger(__name__)

def rebuild_cache():
    nuke_cache()
    warm_up_cache()

def clear_all_geo_caches():
    from .geo_utils import COUNTRY_CODES
    cache_roots = ["activity_mixin_by_country",
                    "activity_mixin_orgs_by_activity_where",
                    "based_in_geo_mixin_based_in_country"]
    for cache_root in cache_roots:
        keys = [f"{cache_root}_{x}" for x in COUNTRY_CODES.keys()]
        cache.delete_many(keys)
    clear_stats_cache(1)

def clear_stats_cache(days_ago = 1):
    cache_keys = [f"stats_{date.today()}"]
    for x in range(1, days_ago + 1):
        cache_keys.append(f"stats_{date.today() - timedelta(days=x)}")
    logger.debug(f"Will delete {cache_keys}")
    cache.delete_many(cache_keys)

def nuke_cache():
    cache.clear()

def warm_up_cache():
    from .geo_utils import COUNTRY_CODES
    for country_code in COUNTRY_CODES.keys():
        logger.info(f"Warming up {country_code}")
        BasedInGeoMixin.based_in_country(country_code,allowed_to_set_cache=True)
        ActivityMixin.orgs_by_activity_where(country_code,allowed_to_set_cache=True)
    logger.info("Warming up stats")
    get_stats(date.today(),allowed_to_set_cache=True)
