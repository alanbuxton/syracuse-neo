from django.core.cache import cache
from topics.models import BasedInGeoMixin, ActivityMixin
from topics.model_queries import get_stats
import logging
from datetime import date, timedelta
logger = logging.getLogger(__name__)

def clear_all_geo_caches():
    from .geo_utils import COUNTRY_CODES
    cache_roots = ["activity_mixin_by_country",
                    "activity_mixin_orgs_by_activity_where",
                    "based_in_geo_mixin_based_in_country"]
    for cache_root in cache_roots:
        keys = [f"{cache_root}_{x}" for x in COUNTRY_CODES.keys()]
        cache.delete_many(keys)

    stats_cache_today = f"stats_{date.today()}"
    stats_cache_yesterday = f"stats_{date.today() - timedelta(days=1)}"
    cache.delete_many([stats_cache_today, stats_cache_yesterday])

def warm_up_cache():
    from .geo_utils import COUNTRY_CODES
    for country_code in COUNTRY_CODES.keys():
        logger.info(f"Warming up {country_code}")
        BasedInGeoMixin.based_in_country(country_code)
        ActivityMixin.orgs_by_activity_where(country_code)
    logger.info("Warming up stats")
    get_stats(date.today())
