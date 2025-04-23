from .region_hierarchies import COUNTRY_CODE_TO_NAME
from .geoname_mappings import CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX
from .orgs_by_industry_geo import (warm_up_all_industry_geos, 
                                   orgs_by_industry_cluster_and_geo,
                                   orgs_by_industry_text_and_geo)
from topics.models import IndustryCluster
from topics.util import geo_to_country_admin1
from .geo_rdf_post_processor import update_geonames_locations_with_country_admin1
import logging
from django.core.cache import cache
logger = logging.getLogger(__name__)

def update_organization_data():
    update_geonames_locations_with_country_admin1()
    warm_up_all_industry_geos()

def orgs_by_industry_and_or_geo(industry_or_id,geo_code,return_orgs_only=False):
    if industry_or_id is None:
        ind_uri = None
        ind_topic_id = None
    elif isinstance(industry_or_id, IndustryCluster):
        ind_uri = industry_or_id.uri 
        ind_topic_id = industry_or_id.topicId
    else:
        ind = IndustryCluster.get_by_industry_id(industry_or_id)
        ind_uri = ind.uri if ind else None
        ind_topic_id = ind.topicId if ind else None
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    orgs_with_rel_counts = orgs_by_industry_cluster_and_geo(ind_uri,ind_topic_id,country_code,admin1_code=admin1_code)
    if return_orgs_only is True:
        return [x[0] for x in orgs_with_rel_counts]
    else:
        return orgs_with_rel_counts

def country_admin1_full_name(geo_code):
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    if country_code is None or country_code == '':
        return ""
    country_name = COUNTRY_CODE_TO_NAME.get(country_code)
    if country_name is None:
        return None
    if admin1_code is None:
        return country_name
    else:
        admin1_name = cache.get(f"{CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX}{geo_code}")
        return f"{country_name} - {admin1_name}"
    
def orgs_by_industry_text_and_geo_code(industry_text, geo_code,return_orgs_only=False):
    country_code, admin1 = geo_to_country_admin1(geo_code)
    res = orgs_by_industry_text_and_geo(industry_text, country_code, admin1)
    if return_orgs_only is True:
        return [x[0] for x in res]
    else:
        return res
