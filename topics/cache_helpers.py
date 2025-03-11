from datetime import datetime, date
from .stats_helpers import get_stats
from topics.industry_geo.geoname_mappings import prepare_country_mapping
from topics.industry_geo import update_organization_data
from django.core.cache import cache
import redis

import logging
logger = logging.getLogger(__name__)


def refresh_geo_data(max_date = date.today()):
    t1 = datetime.now()
    nuke_cache()
    res0 = prepare_country_mapping()
    update_organization_data()
    get_stats(max_date)
    cache.set("activity_stats_last_updated",max_date)
    t2 = datetime.now()
    logger.info(f"Refreshed geo data in {t2 - t1}")
    return res0

def nuke_cache():
    r = redis.Redis()
    r.flushdb()
