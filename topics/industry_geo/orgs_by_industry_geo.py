from topics.models import IndustryCluster, Organization
from topics.industry_geo.geoname_mappings import (COUNTRIES_WITH_STATE_PROVINCE,
    admin1s_for_country)
from topics.industry_geo.region_hierarchies import (GLOBAL_REGION_TO_COUNTRY, 
    COUNTRY_TO_GLOBAL_REGION, US_REGIONS_TO_STATES)
from neomodel import db
from django.core.cache import cache
from collections import defaultdict, OrderedDict
from .hierarchy_utils import filtered_hierarchy, hierarchy_widths
from typing import List

import logging
logger = logging.getLogger(__name__)

def orgs_by_industry_cluster_and_geo(industry_cluster_uri, industry_cluster_topic_id, 
                                     country_code, admin1_code=None, 
                                     force_update_cache=False,
                                     limit=None
                                     ):
    cache_key = f"orgs_industry_cluster_geo_{industry_cluster_topic_id}_{country_code}_{admin1_code}_{limit}"
    
    if force_update_cache is False:
        res = cache.get(cache_key)
        if res is not None:
            return res
    res = do_org_geo_industry_cluster_query(industry_cluster_uri,country_code,admin1_code)
    cache.set(cache_key, res)
    return res

def warm_up_all_industry_geos(force_update_cache=False):
    logger.info("Warming up Industry Geos")
    for ind in IndustryCluster.leaf_nodes_only():
        logger.info(f"Warming up {ind.uri}")
        for cc in COUNTRY_TO_GLOBAL_REGION.keys():
            orgs_by_industry_cluster_and_geo(ind.uri,ind.topicId,
                                             cc,force_update_cache=force_update_cache,
                                            )
            if cc in COUNTRIES_WITH_STATE_PROVINCE:
                for adm1 in (admin1s_for_country(cc) or []):
                    orgs_by_industry_cluster_and_geo(ind.uri,
                                                     ind.topicId,cc,adm1,
                                                     force_update_cache=force_update_cache,
                                                     )


def orgs_by_industry_text_and_geo(industry_text, country_code, admin1_code=None):
    cache_key = f"orgs_industry_text_{hash(industry_text)}_{country_code}"
    if admin1_code is not None:
        cache_key = f"{cache_key}_{admin1_code}"
    res = cache.get(cache_key)
    if res:
        return res
    res = Organization.by_industry_text_and_geo(industry_text, country_code, admin1_code)
    cache.set(cache_key, res, 60*60) # 1 hour cache timeout
    return res

def do_org_geo_industry_cluster_query(industry_uri: str, country_code: str, admin1_code: str, limit: int = None):
    industry_str = f" AND i.uri = '{industry_uri}' " if industry_uri else ""
    country_str = f" AND l.countryCode = '{country_code}' " if country_code else ""
    admin1_str = f" AND l.admin1Code = '{admin1_code}' " if admin1_code else ""
    limit_str = f" LIMIT {limit} " if limit else ""
    query = f"""MATCH (l: Resource&GeoNamesLocation)-[basedInHighGeoNamesLocation]-(o:Resource&Organization)-[industryClusterPrimary]-(i: Resource&IndustryCluster)
                WHERE o.internalMergedSameAsHighToUri IS NULL
                {industry_str}
                {country_str}
                {admin1_str}
                RETURN DISTINCT o.uri
                {limit_str}
                """
    logger.debug(query)
    res, _ = db.cypher_query(query, resolve_objects=True)
    return [x[0] for x in res]

def org_geo_industry_cluster_query_by_words(search_text: str,counts_only):
    industry_clusters = IndustryCluster.by_representative_doc_words(search_text)
    country_results = {}
    adm1_results = defaultdict(dict)
    relevant_countries = []
    relevant_admin1s = defaultdict(list)
    for ind in industry_clusters:
        ind_uri = ind.uri
        country_results[ind_uri] = {}
        for cc in COUNTRY_TO_GLOBAL_REGION.keys():
            res = orgs_by_industry_cluster_and_geo(ind.uri,ind.topicId,cc)
            if len(res) > 0:
                relevant_countries.append(cc)
                if counts_only:
                    res = len(res)
                country_results[ind_uri][cc] = res
                if cc in COUNTRIES_WITH_STATE_PROVINCE:
                    admin1_list = admin1s_for_country(cc)
                    assert admin1_list is not None, f"No admin1 found for {cc}"
                    adm1_results[ind_uri][cc] = {}
                    for adm1 in admin1_list:
                        res = orgs_by_industry_cluster_and_geo(ind.uri,
                                                        ind.topicId,cc,adm1)
                        if len(res) > 0:
                            if counts_only:
                                res = len(res)
                            adm1_results[ind_uri][cc][adm1] = res
                            relevant_admin1s[cc].append(adm1)
    return industry_clusters, relevant_countries, relevant_admin1s, country_results, adm1_results

def org_geo_industry_text_by_words(search_str: str,counts_only):
    country_results = {}
    adm1_results = defaultdict(dict)
    relevant_countries = []
    relevant_admin1s = defaultdict(list)
    for cc in COUNTRY_TO_GLOBAL_REGION.keys():
        res = orgs_by_industry_text_and_geo(search_str,cc)
        if len(res) > 0:
            relevant_countries.append(cc)
            if counts_only:
                res = len(res)
            country_results[cc] = res
            if cc in COUNTRIES_WITH_STATE_PROVINCE:
                admin1_list = admin1s_for_country(cc)
                assert admin1_list is not None, f"No admin1 found for {cc}"
                adm1_results[cc] = {}
                for adm1 in admin1_list:
                    res = orgs_by_industry_text_and_geo(search_str,cc,adm1)
                    if len(res) > 0:
                        if counts_only:
                            res = len(res)
                        adm1_results[cc][adm1] = res
                        relevant_admin1s[cc].append(adm1)
    return relevant_countries, relevant_admin1s, country_results, adm1_results

def combined_industry_geo_results(search_str,counts_only=True):
    ind_clusters, ind_cluster_countries, ind_cluster_admin1s_tmp, ind_cluster_by_country, ind_cluster_by_adm1 = org_geo_industry_cluster_query_by_words(search_str, counts_only)
    text_countries, text_admin1s_tmp, text_by_country, text_by_adm1 = org_geo_industry_text_by_words(search_str, counts_only)

    countries = set(ind_cluster_countries + text_countries)

    admin1s = defaultdict(set)
    for k,v in ind_cluster_admin1s_tmp.items():
        admin1s[k].update(v)    
    for k, v in text_admin1s_tmp.items():
        admin1s[k].update(v)

    country_hierarchy, country_widths, admin1_hierarchy, admin1_widths = build_region_hierarchy(countries, admin1s)
    headers = prepare_headers(country_hierarchy, country_widths, admin1_hierarchy, admin1_widths)
    empty_value = 0 if counts_only is True else None
    ind_cluster_rows, text_row = prepare_rows(headers[6], ind_clusters, ind_cluster_by_country, ind_cluster_by_adm1, 
                        search_str, text_by_country, text_by_adm1, empty_value)
    
    return headers, ind_cluster_rows, text_row

def prepare_rows(header_row, ind_clusters, ind_cluster_by_country, ind_cluster_by_adm1, 
                        search_str, text_by_country, text_by_adm1,empty_value):
    ind_cluster_rows = []
    for ind_cluster in ind_clusters:
        ind_uri = ind_cluster.uri
        row = [ ind_cluster.uri, ind_cluster.longest_representative_doc ]
        for loc in header_row:
            val = get_val_for_country_admin1(loc, ind_cluster_by_country[ind_uri],
                                             ind_cluster_by_adm1[ind_uri],empty_value)
            row.append(val)
        ind_cluster_rows.append(row)

    text_row = [ '', search_str ]
    for loc in header_row:
        val = get_val_for_country_admin1(loc, text_by_country,text_by_adm1,empty_value)
        text_row.append(val)
    return ind_cluster_rows, text_row

def get_val_for_country_admin1(loc, country_data, admin1_data, empty_value):
    search_loc = loc.replace("REPEATED","")
    search_loc = search_loc.replace("n/a","")
    search_loc = search_loc.strip()
    if "-" in search_loc:
        country, admin1 = search_loc.split("-")
        val = admin1_data.get(country,{}).get(admin1,empty_value)
    else:
        val = country_data.get(search_loc,empty_value)
    return val

def prepare_headers(country_hierarchy, country_widths, admin1_hierarchy, admin1_widths):
    '''
    Returns:
        headers: list of dicts, one per header row. dict has value and width
    '''

    results = [] 

    row1 = OrderedDict() # top region
    row2 = OrderedDict() # sub-region 
    row3 = OrderedDict() # intermediate region (may be blank)
    row4 = OrderedDict() # country
    row5 = OrderedDict() # us region
    row6 = OrderedDict() # us division 
    row7 = OrderedDict() # state/province
    for region1, region1_vals in sorted(country_hierarchy.items()):
        row1[region1] = global_region_width( [region1], country_widths, admin1_widths)
        for region2, region2_vals in region1_vals.items():
            col_width = global_region_width( [region1,region2], country_widths, admin1_widths)
            row2[region2] = col_width
            if isinstance(region2_vals, dict):
                # There is a row3
                for region3, region3_vals in region2_vals.items():
                    row3[region3] = global_region_width( [region1,region2, region3], country_widths, admin1_widths)
                    for country in region3_vals:
                        col_width = admin1_width_for_country(country, admin1_widths)
                        row4[country] = col_width
                        if col_width == 1:
                            row5[f"REPEATED {country}"] = 1
                            row6[f"REPEATED {country}"] = 1
                            row7[f"REPEATED {country}"] = 1
                            continue
                        row5[f"{country} n/a"] = 1
                        row6[f"{country} n/a"] = 1
                        row7[f"{country} n/a"] = 1
                        if country == "US":
                            us_data = admin1_hierarchy["US"]
                            for us_region,vals in sorted(us_data.items()):
                                row5[us_region] = admin1_widths["US"][f"US#{us_region}"]
                                for us_division, states in sorted(vals.items()):
                                    row6[us_division] = admin1_widths["US"][f"US#{us_region}#{us_division}"]
                                    for state in states:
                                        row7[f"{country}-{state}"] = 1
                        else:
                            for admin1 in admin1_hierarchy[country]:
                                row5[f"{country} n/a"] = 1
                                row6[f"{country} n/a"] = 1
                                row7[f"{country}-{admin1}"] = 1
            else:
                row3[f"REPEATED {region2}"] = col_width
                for country in region2_vals:
                    col_width = admin1_width_for_country(country, admin1_widths)
                    row4[country] = col_width
                    if col_width == 1:
                        row5[f"REPEATED {country}"] = 1
                        row6[f"REPEATED {country}"] = 1
                        row6[f"REPEATED {country}"] = 1
                        row7[f"REPEATED {country}"] = 1
                        continue
                    row5[f"{country} n/a"] = 1
                    row6[f"{country} n/a"] = 1
                    row7[f"{country} n/a"] = 1
                        
                    if country == "US":
                        us_data = admin1_hierarchy["US"]
                        for us_region,vals in sorted(us_data.items()):
                            row5[us_region] = admin1_widths["US"][f"US#{us_region}"]
                            for us_division, states in sorted(vals.items()):
                                row6[us_division] = admin1_widths["US"][f"US#{us_region}#{us_division}"]
                                for state in states:
                                    row7[f"{country}-{state}"] = 1
                    else:
                        for admin1 in admin1_hierarchy[country]:
                            row5[f"{country} n/a"] = 1
                            row6[f"{country} n/a"] = 1
                            row7[f"{country}-{admin1}"] = 1

    results.append(row1)   
    results.append(row2)
    results.append(row3)
    results.append(row4)
    results.append(row5)
    results.append(row6)
    results.append(row7)
    return results

def global_region_width(regions: List, country_widths, admin1_widths):
    width_key = "#".join(regions)
    col_width = country_widths[width_key]
    for k in admin1_widths.keys():
        if country_in_region(k, regions[-1]):
            col_width += admin1_widths[k][k]
    return col_width

def admin1_width_for_country(country, admin1_widths):
    country_admin1_widths = admin1_widths.get(country)
    if country_admin1_widths is not None:
        admin1_width = country_admin1_widths[country] + 1
    else:
        admin1_width = 1
    return admin1_width

def country_in_region(country, region, country_to_global_region=COUNTRY_TO_GLOBAL_REGION):
    regions = country_to_global_region[country].values()
    if region in regions:
        return True
    else:
        return False

def build_region_hierarchy(countries, admin1s):
    '''

    Args:
        countries: A list of country codes .
        admin1s: A dict of country_code -> list of admin1s.

    Returns:
        country_hierarchy
        country_widths
        admin1_hierarchy
        admin1_widths

    '''
    admin1_hierarchy = {}
    admin1_widths = {}
    for admin1, provinces in admin1s.items():
        if admin1 == "US":
            us_hierarchy = filtered_hierarchy(US_REGIONS_TO_STATES,provinces)
            us_widths = hierarchy_widths({"US":us_hierarchy})
            admin1_widths[admin1] = us_widths
            admin1_hierarchy[admin1] = us_hierarchy
            
        else:
            admin1_widths[admin1] = {admin1: len(provinces)}
            admin1_hierarchy[admin1] = sorted(provinces)

    relevant_countries = {k:v for k,v in COUNTRY_TO_GLOBAL_REGION.items() if k in countries}
    country_hierarchy = filtered_hierarchy(GLOBAL_REGION_TO_COUNTRY, relevant_countries)
    country_widths = hierarchy_widths(country_hierarchy)

    return country_hierarchy, country_widths, admin1_hierarchy, admin1_widths
    



