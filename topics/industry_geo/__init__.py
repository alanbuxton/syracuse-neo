from .region_hierarchies import COUNTRY_CODE_TO_NAME
from .geoname_mappings import CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX, GEO_PARENT_CHILDREN
from .orgs_by_industry_geo import (warm_up_all_industry_geos, 
                                   org_uris_by_industry_cluster_and_geo,
                                   orgs_by_industry_text_and_geo)
from topics.models import IndustryCluster, Resource
from topics.util import geo_to_country_admin1
from .geo_rdf_post_processor import update_geonames_locations_with_country_admin1
import logging
from topics.organization_search_helpers import remove_same_as_name_onlies
from django.core.cache import cache
logger = logging.getLogger(__name__)

def update_organization_data():
    update_geonames_locations_with_country_admin1()
    warm_up_all_industry_geos()

def org_uris_by_industry_and_or_geo(industry_or_id,geo_code,return_orgs_only=False,
                                combine_same_as_name_only=True):
    logger.debug(f"org_uris_by_industry_and_or_geo Ind: {industry_or_id} Geo: {geo_code} Orgs only: {return_orgs_only} Combine sano: {combine_same_as_name_only}")
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
    orgs_with_rel_counts = org_uris_by_industry_cluster_and_geo(ind_uri,ind_topic_id,country_code,admin1_code=admin1_code)
    if combine_same_as_name_only is True:
        orgs_with_rel_counts = remove_same_as_name_onlies([(Resource.get_by_uri(x),y) for x,y in orgs_with_rel_counts])
        orgs_with_rel_counts = [(x.uri, y) for x, y in orgs_with_rel_counts]
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
    
def org_uris_by_industry_text_and_geo_code(industry_text, geo_code,return_orgs_only=False,
                                       combine_same_as_name_only=True):
    country_code, admin1 = geo_to_country_admin1(geo_code)
    orgs_with_rel_counts = orgs_by_industry_text_and_geo(industry_text, country_code, admin1)
    if combine_same_as_name_only is True:
        orgs_with_rel_counts = remove_same_as_name_onlies([(Resource.get_by_uri(x),y) for x,y in orgs_with_rel_counts])
        orgs_with_rel_counts = [(x.uri, y) for x, y in orgs_with_rel_counts]
    if return_orgs_only is True:
        return [x[0] for x in orgs_with_rel_counts]
    else:
        return orgs_with_rel_counts

def geo_codes_for_region(start_region, parent_child=GEO_PARENT_CHILDREN):
    country_admin1s = set()
    def gather_country_admin1s(label):
        if len(label) == 2: # At country level so no need to go further
            country_admin1s.add(label)
            return
        node = parent_child[label] 
        children = node["children"]
        if len(children) == 0:
            country_admin1s.add(label)
        for child in children:
            if len(child) == 2:
                country_admin1s.add(child) # We started higher than country and got to country code no need to go further
            else:
                gather_country_admin1s(child)
    gather_country_admin1s(start_region)
    return country_admin1s
