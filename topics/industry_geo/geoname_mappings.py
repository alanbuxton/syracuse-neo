import csv
import logging
from django.core.cache import cache
from collections import defaultdict
from .region_hierarchies import COUNTRIES_WITH_STATE_PROVINCE
from neomodel import db
from topics.util import cache_friendly

logger = logging.getLogger(__name__)

COUNTRY_TO_ADMIN1_PREFIX = "country_to_admin1_" 
GEO_DATA_PREFIX = "geodata_"
CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX = "country_admin1_to_name_" # key = US-TX etc
CC_ADMIN1_NAME_TO_ADMIN1_CODE_PREFIX = "country_to_name_admin1_" # key = USTexas


def get_geo_data(geonameid):
    '''
        Returns: dict of country, admin1, feature, country_list
    '''
    return cache.get(f"{GEO_DATA_PREFIX}{geonameid}")

def admin1s_for_country(country_code):
    return cache.get(f"{COUNTRY_TO_ADMIN1_PREFIX}{country_code}")

def get_available_geoname_ids():
    res, _ = db.cypher_query("MATCH (n: GeoNamesLocation) RETURN DISTINCT(n.geoNamesId)")
    flattened = [x for sublist in res for x in sublist]
    return flattened

def prepare_country_mapping(fpath="dump/relevant_geo.csv", 
                            countries_for_admin1=COUNTRIES_WITH_STATE_PROVINCE):
    logger.info("Started loading geonames")
    existing_geonames = get_available_geoname_ids()
    cnt = 0
    geonameid_data = {}
    country_code_to_admin1 = defaultdict(set)
    cc_admin1_code_to_admin1_name = {} # key = tuple of cc_code and admin1_code
    cc_code_admin1_name_to_admin1_code = {} # key = tuple of cc_code and admin1 name
    with open(fpath,"r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cnt += 1
            if cnt % 1_000_000 == 0:
                logger.info(f"Processed: {cnt} records.")
            geo_id = row['geonameid']
            geo_id = int(geo_id)
            if geo_id not in existing_geonames:
                continue
            cc = row['country_code']
            fc = row['feature_code']
            admin1 = row['admin1_code']
            cc2 = row['cc2'] or ''
            cc_list = cc2.split(",")
            if cc_list == ['']:
                cc_list = None
            geonameid_data[geo_id] = {
                "country": cc,
                "feature": fc,
                "admin1": admin1,
                "country_list": cc_list,
            }
            cache.set(f"{GEO_DATA_PREFIX}{geo_id}",geonameid_data[geo_id])               
            if cc in countries_for_admin1:
                if fc == 'ADM1' and admin1 != '' and admin1 != '00': # The code '00' stands for 'we don't know the official code'. https://forum.geonames.org/gforum/posts/list/703.page
                    country_code_to_admin1[cc].add(admin1)
                    admin1_name = row['name']
                    cache.set( f"{CC_ADMIN1_CODE_TO_ADMIN1_NAME_PREFIX}{cc}-{admin1}", admin1_name)                
                    cache.set( cache_friendly( f"{CC_ADMIN1_NAME_TO_ADMIN1_CODE_PREFIX}{cc}-{admin1_name}"), admin1)
    for k,vs in country_code_to_admin1.items():
        cache.set(f"{COUNTRY_TO_ADMIN1_PREFIX}{k}",list(vs))
    logger.info(f"Processed: {cnt} records.")
    return geonameid_data, country_code_to_admin1

