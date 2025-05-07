from datetime import datetime, date
from .stats_helpers import get_stats, warm_up_activities_by_org
from topics.industry_geo.geoname_mappings import prepare_country_mapping
from topics.industry_geo import update_organization_data
from django.core.cache import cache
import redis
from topics.util import min_and_max_date

import logging
logger = logging.getLogger(__name__)


def refresh_geo_data(max_date = date.today(),also_warm_up_activities=True):
    t1 = datetime.now()
    _, max_date = min_and_max_date({"max_date":max_date})
    logger.info(f"Resetting cache as at {max_date}")
    nuke_cache()
    prepare_country_mapping()
    update_organization_data()
    get_stats(max_date)
    cache.set("activity_stats_last_updated",max_date)
    t2 = datetime.now()
    logger.info(f"Refreshed geo data in {t2 - t1}")
    if also_warm_up_activities is True:
        do_warm_up_activities_by_org(max_date)
    return max_date

def do_warm_up_activities_by_org(max_date):
    t1 = datetime.now()
    min_date, max_date = min_and_max_date({"max_date":max_date})
    warm_up_activities_by_org(min_date,max_date)
    t2 = datetime.now()
    logger.info(f"Refreshed activities by org in {t2 - t1}")
    return min_date, max_date


def nuke_cache():
    r = redis.Redis()
    r.flushdb()

