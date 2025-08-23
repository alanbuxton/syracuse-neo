from topics.industry_geo.region_hierarchies import COUNTRY_CODE_TO_NAME
from topics.industry_geo.geoname_mappings import CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX, geo_parent_children
from topics.util import geo_to_country_admin1
import logging
from syracuse.cache_util import get_versionable_cache
logger = logging.getLogger(__name__)


def country_admin1_full_name(geo_code, version=None):
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    if country_code is None or country_code == '':
        return ""
    country_name = COUNTRY_CODE_TO_NAME.get(country_code)
    if country_name is None:
        return None
    if admin1_code is None:
        return country_name
    else:
        admin1_name = get_versionable_cache(f"{CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX}{geo_code}", version)
        return f"{country_name} - {admin1_name}"


def geo_codes_for_region(start_region, parent_child=None):
    if parent_child is None:
        parent_child = geo_parent_children()
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
