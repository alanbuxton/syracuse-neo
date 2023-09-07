import csv
from .models import Resource, geonames_uris
import logging
import pycountry

logger = logging.getLogger("syracuse")

POLITICAL_ENTITY_FEATURE_CODES = set(["PCL","PCLD","PCLF","PCLH","PCLI","PCLIX","PCLS"])

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
    existing_geonames_ids = geonames_uris()
    country_mapping = load_filtered_country_mapping(fpath, set(existing_geonames_ids))
    country_names = {}
    for key in country_mapping.keys():
        country = pycountry.countries.get(alpha_2=key)
        country_names[country.name] = key
    return country_names, country_mapping
