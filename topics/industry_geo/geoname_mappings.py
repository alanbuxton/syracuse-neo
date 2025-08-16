import csv
import logging
from collections import defaultdict
from topics.industry_geo.region_hierarchies import (COUNTRIES_WITH_STATE_PROVINCE, 
    US_REGIONS_TO_STATES_HIERARCHY, GLOBAL_REGION_TO_COUNTRY
)
from neomodel import db
from syracuse.cache_util import set_versionable_cache, get_versionable_cache

logger = logging.getLogger(__name__)

COUNTRY_TO_ADMIN1_PREFIX = "country_to_admin1_" 
GEO_DATA_PREFIX = "geodata_"
CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX = "cc_adm1code_to_adm1name_" # key = US-TX etc
CC_ADMIN1_NAME_TO_ADMIN1_CODE_PREFIX = "cc_adm1name_to_adm1code_" #  key = US_Texas


def get_geo_data(geonameid,version=None):
    '''
        Returns: dict of country, admin1, feature, country_list
    ''' 
    return get_versionable_cache(f"{GEO_DATA_PREFIX}{geonameid}", version)

def admin1s_for_country(country_code, version=None):
    return get_versionable_cache(f"{COUNTRY_TO_ADMIN1_PREFIX}{country_code}", version)

def get_available_geoname_ids():
    res, _ = db.cypher_query("MATCH (n: GeoNamesLocation) RETURN DISTINCT(n.geoNamesId)")
    flattened = [x for sublist in res for x in sublist]
    return set(flattened)

def prepare_country_mapping(version=None,
                            fpath="dump/relevant_geo.csv", 
                            countries_for_admin1=COUNTRIES_WITH_STATE_PROVINCE):
    logger.info("Started loading geonames")
    existing_geonames = get_available_geoname_ids()
    cnt = 0
    geonameid_data = {}
    country_code_to_admin1 = defaultdict(set)
    with open(fpath,"r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cnt += 1
            if cnt % 1_000_000 == 0:
                logger.info(f"Processed: {cnt} records.")
            geo_id = row['geonameid']
            geo_id = int(geo_id)
            cc = row['country_code']
            fc = row['feature_code']
            admin1 = row['admin1_code']
            cc2 = row['cc2'] or ''
            cc_list = cc2.split(",")
            if cc_list == ['']:
                cc_list = None
            if geo_id in existing_geonames:
                geonameid_data[geo_id] = {
                    "country": cc,
                    "feature": fc,
                    "admin1": admin1,
                    "country_list": cc_list,
                }
                set_versionable_cache(f"{GEO_DATA_PREFIX}{geo_id}",geonameid_data[geo_id], version)               
            if fc == 'ADM1' and cc in countries_for_admin1 and admin1 != '' and admin1 != '00': # The code '00' stands for 'we don't know the official code'. https://forum.geonames.org/gforum/posts/list/703.page
                country_code_to_admin1[cc].add(admin1)
                admin1_name = row['name']
                set_versionable_cache( f"{CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX}{cc}-{admin1}", admin1_name, version)                
                set_versionable_cache( f"{CC_ADMIN1_NAME_TO_ADMIN1_CODE_PREFIX}{cc}-{admin1_name}", admin1, version)
    for k,vs in country_code_to_admin1.items():
        set_versionable_cache(f"{COUNTRY_TO_ADMIN1_PREFIX}{k}",list(vs),version)
    logger.info(f"Processed: {cnt} records.")
    return geonameid_data, country_code_to_admin1


def region_parent_child(version):
    parent_child = {}

    def iterate_through_global_regions(parent_region,current_region_and_lower):
        for region, lower_region_or_countries in current_region_and_lower.items():
            parent_child[region] = {"parent":parent_region,"id":region, "children":set()}
            if parent_region is not None:
                parent_child[parent_region]["children"].add(region)
            if isinstance(lower_region_or_countries, dict):
                iterate_through_global_regions(region, lower_region_or_countries)
            else:
                parent_child[region]["children"] = lower_region_or_countries
                for child in lower_region_or_countries:
                    parent_child[child] = {"parent":region, "id":child, "children":set()}

    iterate_through_global_regions(None, GLOBAL_REGION_TO_COUNTRY)

    for us_region, sub_region_and_states in US_REGIONS_TO_STATES_HIERARCHY.items():
        parent_child["US"]["children"].add(us_region)
        parent_child[us_region] = {"parent":"US","id": us_region, "children":set()}
        for sub_region, states in sub_region_and_states.items():
            state_strs = [f"US-{x}" for x in states]
            parent_child[sub_region] = {"parent": us_region, "id":sub_region, "children": state_strs}
            parent_child[us_region]["children"].add(sub_region)
            for state in state_strs:
                parent_child[state] = {"parent": sub_region, "id": state, "children": set()}

    for country in COUNTRIES_WITH_STATE_PROVINCE:
        if country == 'US':
            continue
        state_strs = [f"{country}-{x}" for x in (admin1s_for_country(country, version) or [])]
        for state in state_strs:
            parent_child[country]["children"].add(state)
            parent_child[state] = {"parent":country,"id":state, "children":set()}
            
    return parent_child

def geo_parent_children(version=None):
    cache_key = "geo_parent_children"
    res = get_versionable_cache(cache_key,version)
    if res is not None:
        return res
    res = region_parent_child(version)
    set_versionable_cache(cache_key, res, version)
    return res


