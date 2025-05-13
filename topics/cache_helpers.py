from datetime import datetime, date
from .stats_helpers import get_stats
from topics.industry_geo.geoname_mappings import prepare_country_mapping
from topics.industry_geo import update_geonames_locations_with_country_admin1
from topics.industry_geo.orgs_by_industry_geo import do_all_precalculations
from django.core.cache import cache
import redis
from topics.util import min_and_max_date

import logging
logger = logging.getLogger(__name__)


def refresh_geo_data(max_date = date.today(),fill_blanks=True):
    t1 = datetime.now()
    _, max_date = min_and_max_date({"max_date":max_date})
    logger.info(f"Resetting cache as at {max_date}")
    nuke_cache()
    prepare_country_mapping()
    update_geonames_locations_with_country_admin1()
    do_all_precalculations(max_date,fill_blanks=fill_blanks)
    if fill_blanks is True:
        get_stats(max_date)
    cache.set("activity_stats_last_updated",max_date)
    t2 = datetime.now()
    logger.info(f"Refreshed geo data in {t2 - t1}")
    return max_date

def nuke_cache():
    r = redis.Redis()
    r.flushdb()

