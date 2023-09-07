import os
import pickle
import logging
logger = logging.getLogger("syracuse")

COUNTRY_NAMES = None
COUNTRY_MAPPING = None
COUNTRY_CODES = None

def load_geo_data():
    global COUNTRY_NAMES
    global COUNTRY_MAPPING
    global COUNTRY_CODES
    cache_file = "tmp/geo_cache.pickle"
    if os.path.isfile(cache_file):
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
