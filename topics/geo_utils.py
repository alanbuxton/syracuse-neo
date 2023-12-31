import os
import pickle
import logging
from .models import Resource, geonames_uris
import pycountry
import csv
logger = logging.getLogger(__name__)

COUNTRY_NAMES = None
COUNTRY_MAPPING = None
COUNTRY_CODES = None
POLITICAL_ENTITY_FEATURE_CODES = set(["PCL","PCLD","PCLF","PCLH","PCLI","PCLIX","PCLS"])

def load_geo_data(force_refresh=False):
    global COUNTRY_NAMES
    global COUNTRY_MAPPING
    global COUNTRY_CODES
    cache_file = "tmp/geo_cache.pickle"
    if force_refresh is False and os.path.isfile(cache_file):
        COUNTRY_NAMES, COUNTRY_MAPPING = load_from_cache(cache_file)
    else:
        COUNTRY_NAMES, COUNTRY_MAPPING = load_country_mapping()
        save_to_cache(cache_file, COUNTRY_NAMES, COUNTRY_MAPPING)
    COUNTRY_CODES = {v:k for k,v in COUNTRY_NAMES.items()}


def load_from_cache(fpath):
    logger.debug(f"Loading country names/mapping from cache file {fpath}")
    with open(fpath, 'rb') as handle:
        d = pickle.load(handle)
        return d["country_names"], d["country_mapping"]

def save_to_cache(fpath,country_names, country_mapping):
    logger.debug(f"Saving country names/mapping to cache file {fpath}")
    d = {"country_names":country_names, "country_mapping":country_mapping}
    with open(fpath, 'wb') as handle:
        pickle.dump(d, handle, protocol=pickle.HIGHEST_PROTOCOL)


def load_filtered_country_mapping(fpath="dump/relevant_geo.csv",existing_geonames_ids=set()):
    country_mapping = {}
    cnt = 0
    with open(fpath,"r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cnt += 1
            if cnt % 100_000 == 0:
                logger.info(f"Processed: {cnt} records. country_mapping length: {len(country_mapping)}")
            cc = row['country_code']
            if cc is None or cc.strip() == '': # not related to a country
                continue
            geo_id = row['geonameid']
            geonames_uri = f"https://sws.geonames.org/{geo_id}/about.rdf"
            if geonames_uri in existing_geonames_ids:
                if cc not in country_mapping:
                    country_mapping[cc] = []
                country_mapping[cc].append(geo_id)
    return country_mapping

def load_country_mapping(fpath="dump/relevant_geo.csv"):
    if not os.path.isfile(fpath):
        raise ValueError(f"{fpath} not found, please check https://github.com/alanbuxton/syracuse-neo/blob/main/dump/README.md")
    existing_geonames_ids = geonames_uris()
    country_mapping = load_filtered_country_mapping(fpath, set(existing_geonames_ids))
    country_names = {}
    for key in country_mapping.keys():
        if key == 'XK':
            country_names['Kosovo'] = key
        elif key == 'YU':
            # historic Yugoslavia
            continue
        else:
            country = pycountry.countries.get(alpha_2=key)
            if country is None:
                raise ValueError(f"Couldn't find country for {key}")
            country_names[country.name] = key
    return country_names, country_mapping
