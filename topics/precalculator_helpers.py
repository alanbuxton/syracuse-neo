from precalculator.models import P
from topics.models import Organization
from topics.model_queries import get_stats
import logging
from datetime import date, datetime, timezone
from topics.geo_utils import get_geo_data
from topics.industry_geo_helpers import get_orgs_by_leaf_industry_clusters
logger = logging.getLogger(__name__)

def precalculate_all():
    P.nuke_all()
    warm_up_precalculator()

def warm_up_precalculator(max_date=date.today()):
    _, _, country_region_codes, geonameid_to_country_regions = get_geo_data(True)
    # for country_region_code in country_region_codes.keys():
    #     logger.info(f"Warming up {country_region_code}")
    #     Organization.by_industry_and_or_geo(None,country_region_code,allowed_to_set_cache=True)
    # logger.info("Warming up stats")
    # get_stats(max_date,allowed_to_set_cache=True)
    get_orgs_by_leaf_industry_clusters(**geonameid_to_country_regions)
    P.set_last_updated(datetime.now(tz=timezone.utc))
