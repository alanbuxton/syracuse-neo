import os
import logging
import pycountry
import csv
from django.core.cache import cache
from neomodel import db
logger = logging.getLogger(__name__)

GEO_CACHE_KEY="geo_data"
COUNTRIES_WITH_REGIONS = ["AE","US","CA","CN","IN"] # Countries to be broken down to state/province

def country_and_region_code_to_name(geo_code):
    _,_,country_admin1_code_to_name = get_geo_data()
    if len(geo_code) == 2:
        return country_admin1_code_to_name[geo_code]
    country = country_admin1_code_to_name[geo_code[:2]]
    region = country_admin1_code_to_name[geo_code[3:]]
    return f"{region} ({country})"

def geoname_ids_for_country_region(geo_code):
    if geo_code is None or geo_code.strip() == '':
        return []
    geo_mapping, _, _ = get_geo_data()
    if "-" not in geo_code:
        relevant_ids = geo_mapping[geo_code]
        if isinstance(relevant_ids, dict) == True:
            return [x for sublist in relevant_ids.values() for x in sublist]
        else:
            return relevant_ids
    country_code = geo_code[:2]
    region_code = geo_code[3:]
    return geo_mapping[country_code][region_code]

def geonames_uris():
    qry = "match (n) where n.uri contains ('sws.geonames.org') return n.uri"
    res,_ = db.cypher_query(qry)
    flattened = [x for sublist in res for x in sublist]
    return flattened

def get_geoname_uris_for_country_region(geo_code):
    geoname_ids = geoname_ids_for_country_region(geo_code)
    geo_uris = [f"https://sws.geonames.org/{x}/about.rdf" for x in geoname_ids]
    return geo_uris

def get_geo_data(force_refresh=False):
    res = cache.get(GEO_CACHE_KEY)
    if force_refresh is False and res is not None:
        country_admin1_geoname_mapping = res["country_admin1_geoname_mapping"]
        country_admin1_name_to_code = res["country_admin1_name_to_code"]
        country_admin1_code_to_name = res["country_admin1_code_to_name"]
    else:
        country_names_to_id, admin1_names_to_id, country_admin1_geoname_mapping = load_country_mapping()
        country_admin1_name_to_code = sorted_country_admin1_list(country_names_to_id, admin1_names_to_id)
        country_admin1_code_to_name = {z:"".join([x,y]) for x,y,z in country_admin1_name_to_code}
        cache.set(GEO_CACHE_KEY, {"country_admin1_geoname_mapping":country_admin1_geoname_mapping,
                                    "country_admin1_name_to_code": country_admin1_name_to_code,
                                    "country_admin1_code_to_name": country_admin1_code_to_name,
                                    })
    return country_admin1_geoname_mapping, country_admin1_name_to_code, country_admin1_code_to_name

def geo_select_list(include_alt_names=False):
    _,rows,_ = get_geo_data()
    select_list = [ ["",""] ]
    for country_name,admin1_name,geo_code in rows:
        if admin1_name == '':
            select_list.append( [geo_code , country_name] )
        else:
            select_list.append( [geo_code, f"{country_name} - {admin1_name}" ])
    if include_alt_names is True:
        select_list.append( ["GB","United Kingdom of Great Britain and Northern Ireland"])
    return sorted(select_list, key = lambda x: x[1])

def sorted_country_admin1_list(country_names_to_id,admin1_names_to_id):
    combined_sorted_list = [] # country name, admin1_name, country_or_admin_code
    for country_name,country_code in sorted(country_names_to_id.items()):
        combined_sorted_list.append( (country_name,'',country_code) )
        if country_code in COUNTRIES_WITH_REGIONS:
            for admin1_name,admin1_code in sorted(admin1_names_to_id[country_code].items()):
                combined_sorted_list.append( (country_name, admin1_name, f"{country_code}-{admin1_code}"))
    return combined_sorted_list

def load_filtered_country_mapping(fpath="dump/relevant_geo.csv",existing_geonames_ids=set()):
    country_admin1_mapping = {} # country_code => {set of geonames or {admin1_code => set of geonames}}
    all_admin1_data = {} # country_code => {name => code}
    cnt = 0
    with open(fpath,"r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cnt += 1
            if cnt % 100_000 == 0:
                logger.info(f"Processed: {cnt} records. country_admin1_mapping length: {len(country_admin1_mapping)}; all admin1_data length: {len(all_admin1_data)}")
            cc = row['country_code']
            if cc is None or cc.strip() == '': # not related to a country
                continue
            geo_id = row['geonameid']
            geonames_uri = f"https://sws.geonames.org/{geo_id}/about.rdf"
            feature_code = row['feature_code']
            if feature_code == 'ADM1':
                admin1_code = row["admin1_code"]
                if cc not in all_admin1_data:
                    all_admin1_data[cc] = {}
                all_admin1_data[cc][admin1_code] = row["name"]
            if geonames_uri in existing_geonames_ids:
                if cc in COUNTRIES_WITH_REGIONS:
                    if cc not in country_admin1_mapping:
                        country_admin1_mapping[cc] = {}
                    admin1_code = row['admin1_code']
                    if admin1_code not in country_admin1_mapping[cc]:
                        country_admin1_mapping[cc][admin1_code] = set()
                    country_admin1_mapping[cc][admin1_code].add(geo_id)
                else:
                    if cc not in country_admin1_mapping:
                        country_admin1_mapping[cc] = set()
                    country_admin1_mapping[cc].add(geo_id)

    return country_admin1_mapping, all_admin1_data

def narrow_admin1_data(country_admin1_to_geonames, all_admin1_data):
    admin1_data = {}
    for k,v in all_admin1_data.items():
        if k in COUNTRIES_WITH_REGIONS and k in country_admin1_to_geonames:
            if k not in admin1_data:
                admin1_data[k] = {}
            for k2, v2 in v.items():
                if k2 in country_admin1_to_geonames[k]:
                    admin1_data[k][v2]=k2
    return admin1_data

def load_country_mapping(fpath="dump/relevant_geo.csv"):
    if not os.path.isfile(fpath):
        raise ValueError(f"{fpath} not found, please check https://github.com/alanbuxton/syracuse-neo/blob/main/dump/README.md")
    existing_geonames_ids = geonames_uris()
    country_admin1_to_used_geonames, all_admin1_code_to_name= load_filtered_country_mapping(fpath, set(existing_geonames_ids))
    admin1_names_to_id = narrow_admin1_data(country_admin1_to_used_geonames, all_admin1_code_to_name)
    country_names_to_id = {}
    for key in country_admin1_to_used_geonames.keys():
        country_code = key
        if country_code == 'XK':
            country_names_to_id['Kosovo'] = country_code
        elif country_code == 'YU':
            # historic Yugoslavia
            continue
        elif country_code == 'CS':
            # deleted Serbia Montenegro
            continue
        else:
            country = pycountry.countries.get(alpha_2=country_code)
            if country is None:
                raise ValueError(f"Couldn't find country for {country_code}")
            country_names_to_id[country.name] = country_code
    return country_names_to_id, admin1_names_to_id, country_admin1_to_used_geonames
