from .region_hierarchies import COUNTRY_CODE_TO_NAME
from .geoname_mappings import CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX
from .orgs_by_industry_geo import warm_up_all_industry_geos, orgs_by_industry_cluster_and_geo
from topics.models import IndustryCluster
from .geo_rdf_post_processor import update_geonames_locations_with_country_admin1
import logging
from django.core.cache import cache
logger = logging.getLogger(__name__)

def update_organization_data():
    update_geonames_locations_with_country_admin1()
    warm_up_all_industry_geos(force_update_cache=True)

def orgs_by_industry_and_or_geo(industry_id,geo_code,limit=None):
    if industry_id is not None:
        ind = IndustryCluster.nodes.get_or_none(topicId=industry_id)
        ind_uri = ind.uri
        ind_topic_id = ind.topicId
    else:
        ind_uri = None
        ind_topic_id = None
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    return orgs_by_industry_cluster_and_geo(ind_uri,ind_topic_id,country_code,admin1_code=admin1_code)

def country_admin_full_name(geo_code):
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    country_name = COUNTRY_CODE_TO_NAME[country_code]
    if admin1_code is None:
        return country_name
    else:
        admin1_name = cache.get(f"{CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX}{geo_code}")
        return f"{country_name} - {admin1_name}"
    
def geo_to_country_admin1(geo_code):
    splitted = geo_code.split("-")
    country_code = splitted[0]
    admin1_code = splitted[1] if len(splitted) > 1 else None
    return country_code, admin1_code


    
