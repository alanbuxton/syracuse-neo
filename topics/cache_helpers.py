from datetime import datetime, date
from .stats_helpers import get_stats
from topics.industry_geo.geoname_mappings import prepare_country_mapping
from topics.industry_geo.geo_rdf_post_processor import update_geonames_locations_with_country_admin1
from topics.industry_geo.orgs_by_industry_geo import do_all_precalculations
from django.core.cache import cache
from syracuse.cache_util import get_inactive_version, set_active_version, set_versionable_cache, nuke_cache
from syracuse.date_util import min_and_max_date

import logging
logger = logging.getLogger(__name__)


def refresh_geo_data(max_date = date.today(),fill_blanks=True,
                     with_reset=False):
    t1 = datetime.now()
    _, max_date = min_and_max_date({"max_date":max_date})
    logger.info(f"Resetting cache as at {max_date}")
    if with_reset is True:
        nuke_cache()
    to_be_version = get_inactive_version()
    prepare_country_mapping(to_be_version) 
    update_geonames_locations_with_country_admin1(to_be_version) 
    do_all_precalculations(to_be_version, max_date, fill_blanks=fill_blanks)
    if fill_blanks is True:
        get_stats(max_date,to_be_version)
    set_versionable_cache("activity_stats_last_updated", max_date, to_be_version)
    set_active_version(to_be_version)
    t2 = datetime.now()
    logger.info(f"Refreshed geo data in {t2 - t1}")
    return max_date


