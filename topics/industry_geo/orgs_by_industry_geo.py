from topics.models import IndustryCluster, Organization
from topics.industry_geo.geoname_mappings import (COUNTRIES_WITH_STATE_PROVINCE,
    admin1s_for_country)
from topics.industry_geo.region_hierarchies import (GLOBAL_REGION_TO_COUNTRY, 
    COUNTRY_TO_GLOBAL_REGION, US_REGIONS_TO_STATES_HIERARCHY,
    GLOBAL_REGION_TO_COUNTRIES_FLAT,
    US_NATIONAL_REGIONS_TO_STATES)
from neomodel import db
from django.core.cache import cache
from collections import defaultdict, OrderedDict
from .hierarchy_utils import filtered_hierarchy, hierarchy_widths
from typing import List
from topics.util import date_minus, cacheable_hash
from topics.util import ALL_ACTIVITY_LIST, cache_friendly, min_and_max_date
from syracuse.settings import GEO_LOCATION_MIN_WEIGHT_PROPORTION, INDUSTRY_CLUSTER_MIN_WEIGHT_PROPORTION
from topics.neo4j_utils import date_to_cypher_friendly, neo4j_date_converter
from topics.industry_geo.geoname_mappings import COUNTRIES_WITH_STATE_PROVINCE, admin1s_for_country
from topics.industry_geo.region_hierarchies import COUNTRY_TO_GLOBAL_REGION
import logging
from datetime import date

logger = logging.getLogger(__name__)


def cached_activity_stats_last_updated_date():
    return cache.get("activity_stats_last_updated")

def get_org_activities_cache_key(min_date,max_date,industry,country,admin1):
    name = f"orgs_acts_{min_date}_{max_date}_{industry}_{country}_{admin1}"
    return cache_friendly(name)

def get_org_activity_counts(min_date,max_date,industry,country_code,admin1):
    acts = get_org_activities(min_date,max_date,industry,country_code,admin1)
    return len(acts)

def get_org_activities(min_date,max_date,industry,country_code,admin1):
    cache_key = get_org_activities_cache_key(min_date,max_date,industry,country_code,admin1)
    org_datas = cache.get(cache_key)
    if org_datas is None:
        raise ValueError(f"No cached stats for {min_date} {max_date} {industry} {country_code} {admin1} (key {cache_key})")
    acts = set()
    for row in org_datas:
        act_datas = row[-1]
        for act_data in act_datas:
            if act_data[0] is not None:
                acts.add(tuple(act_data))
    return sorted(acts, key=lambda x: ( -(x[2].timestamp()), x[0]))

def org_uris_by_industry_id_country_admin1(industry_cluster_topic_id, 
                                     country_code, admin1_code=None,min_date=None, max_date=None):
    if country_code == '':
        country_code = None
    if admin1_code == '':
        admin1_code = None
    if industry_cluster_topic_id == '':
        industry_cluster_topic_id = None
    min_date, max_date = min_and_max_date({"min_date":min_date,"max_date":max_date})  # None max date will use latest cached data 
    cache_key = get_org_activities_cache_key(min_date,max_date,industry_cluster_topic_id,country_code,admin1_code)
    res = cache.get(cache_key)
    if res is None:
        raise ValueError(f"No result for {cache_key}")
    return res

def convert_and_cache_org_results(org_data_l,cache_key):
    '''
    org_data_l is: o.uri, apoc.node.degree(o), o.internalDocId, articleData
    and each articleData is: act.uri, art.uri, art.datePublished

    1. Sort nodes by number of relationships
    2. Convert neo4j datetimes to python for redis
    '''
    org_data = sorted(org_data_l, key=lambda v: (-v[1], v[2], v[0]))
    converted = [
        (uri,rel_count,doc_id, neo4j_date_converter(article_data))
        for uri,rel_count,doc_id,article_data in org_data
    ]
    cache.set(cache_key, converted)
    return converted


def do_all_precalculations(max_date=date.today(),fill_blanks=True):
    days_ago_90 = date_minus(max_date,90)
    days_ago_30 = date_minus(max_date,30)
    days_ago_7 = date_minus(max_date,7)
    for min_date_tmp in days_ago_90, days_ago_30, days_ago_7:
        min_date, max_date = min_and_max_date({"min_date":min_date_tmp,"max_date":max_date})
        logger.info(f"Precalculating activities: {min_date} - {max_date}")
        res = build_and_run_query(min_date, max_date, include_industry=True, include_geo=False)
        for industry_topic_id, org_data_l in res:
            cache_key = get_org_activities_cache_key(min_date,max_date,industry_topic_id,None,None)
            _ = convert_and_cache_org_results(org_data_l, cache_key)
        logger.debug("Industry done")
        res = build_and_run_query(min_date, max_date, include_industry=False, include_geo=True)
        for country_code, org_data_l in res:
            cache_key = get_org_activities_cache_key(min_date,max_date,None,country_code,None)
            _ = convert_and_cache_org_results(org_data_l, cache_key)
        logger.debug("Country done")
        res = build_and_run_query(min_date, max_date, include_industry=True, include_geo=True)
        for industry_topic_id, country_code, org_data_l in res:
            cache_key = get_org_activities_cache_key(min_date,max_date,industry_topic_id,country_code,None)
            _ = convert_and_cache_org_results(org_data_l, cache_key)
        logger.debug("Industry & Country done")
        res = build_and_run_query(min_date, max_date, include_industry=True, include_geo=True, include_admin1=True)
        for industry_topic_id, country_code, admin1_code, org_data_l in res:
            cache_key = get_org_activities_cache_key(min_date,max_date,industry_topic_id,country_code,admin1_code)
            _ = convert_and_cache_org_results(org_data_l, cache_key)
        logger.debug("Industry, Country, Admin1 Done")
        res = build_and_run_query(min_date, max_date, include_industry=False, include_geo=True, include_admin1=True)
        cache_key = get_org_activities_cache_key(min_date,max_date,False,True,True)
        for country_code, admin1_code, org_data_l in res:
            cache_key = get_org_activities_cache_key(min_date,max_date,None,country_code,admin1_code)
            _ = convert_and_cache_org_results(org_data_l, cache_key)
        logger.info("Existing industry/country/admin1 done")
        if fill_blanks is True:
            set_not_found_industry_geo_to_empty_list(min_date, max_date)
            logger.info("Set not found to [] done")

def set_not_found_industry_geo_to_empty_list(min_date, max_date):
    industry_topic_ids = [None] + [x.topicId for x in IndustryCluster.all_leaf_nodes()]
    country_codes = [None] + list(COUNTRY_TO_GLOBAL_REGION.keys())
    for topic_id in industry_topic_ids:
        for country_code in country_codes:
            if topic_id is None and country_code is None:
                continue # Not allowed
            cache_key = get_org_activities_cache_key(min_date,max_date,topic_id,country_code,None)
            res = cache.get(cache_key)
            if res is None:
                cache.set(cache_key, [])
            if country_code in COUNTRIES_WITH_STATE_PROVINCE:
                admin1_list = admin1s_for_country(country_code)
                for adm1 in admin1_list:
                    cache_key = get_org_activities_cache_key(min_date,max_date,topic_id,country_code,adm1)
                    res = cache.get(cache_key)
                    if res is None:
                       cache.set(cache_key, [])

def build_and_run_query(min_date, max_date, include_industry=True, include_geo=True, include_admin1=False):
    query = build_query(min_date, max_date, include_industry, include_geo, include_admin1)
    logger.debug(query)
    vals, _ = db.cypher_query(query)
    return vals

def build_query(min_date, max_date, include_industry, include_geo, include_admin1):
    if include_industry is False and include_geo is False:
        raise ValueError(f"Must use industry or geo")
    if include_geo is False and include_admin1 is True:
        raise ValueError("If include_admin1 is set then also need to have include_geo set")
    industry_section = build_industry_section() if include_industry else ""
    geo_section = build_geo_section(include_admin1) if include_geo else ""
    query = f"""
        MATCH (o:Resource&Organization)
        WHERE o.internalMergedSameAsHighToUri IS NULL
        {industry_section}
        {geo_section}
        {build_article_section(min_date,max_date)}
        {build_return_stmt(include_industry, include_geo, include_admin1)}
    """
    return query

def build_return_stmt(include_industry, include_geo, include_admin1):
    to_return = []
    if include_industry:
        to_return.append("ic.topicId")
    if include_geo:
        to_return.append("loc.countryCode")
    if include_admin1:
        to_return.append("loc.admin1Code")
    assert len(to_return) > 0, f"{to_return} has to have at least one item to return"
    stmt = f"RETURN {', '.join(to_return)} , collect(distinct([o.uri, apoc.node.degree(o), o.internalDocId, articleData]))"
    return stmt


def build_industry_section():
    return f"""CALL {{
        WITH o
        MATCH (o)-[r:industryClusterPrimary]->(ic:IndustryCluster)
        WITH o, ic, r.weight AS weight
        MATCH (o)-[rAll:industryClusterPrimary]->(:IndustryCluster)
        WITH o, ic, weight, SUM(rAll.weight) AS totalWeight
        WHERE weight >= {INDUSTRY_CLUSTER_MIN_WEIGHT_PROPORTION} * totalWeight
        AND weight > 1
        RETURN ic
    }}
    """

def build_geo_section(include_admin1=False):
    admin1_section = build_admin1_section() if include_admin1 else ""
    return f"""CALL {{
        WITH o
        MATCH (o)-[r:basedInHighGeoNamesLocation]->(loc:GeoNamesLocation)
        {admin1_section}
        WITH o, loc, r.weight AS weight
        MATCH (o)-[rAll:basedInHighGeoNamesLocation]->(:GeoNamesLocation)
        WITH o, loc, weight, SUM(rAll.weight) AS totalWeight
        WHERE weight >= {GEO_LOCATION_MIN_WEIGHT_PROPORTION} * totalWeight
        AND weight > 1
        RETURN loc
    }}
    """
        
def build_admin1_section():
    return f"""WHERE loc.countryCode in {COUNTRIES_WITH_STATE_PROVINCE}
               AND loc.admin1Code <> '00'"""

def build_article_section(min_date, max_date):
    '''
        Any activities related to this org where the org wasn't a participant. E.g. legal firms will often be participants
        but the industry of the buyer/seller is something completely different.
    '''
    return f"""
    CALL {{
    WITH o
    OPTIONAL MATCH (act:{ALL_ACTIVITY_LIST})-[rel]-(o)-[:documentSource]->(art: Article)<-[:documentSource]-(act)
    WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
    AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
    AND TYPE(rel) <> 'participant'
    AND act.internalMergedActivityWithSimilarRelationshipsToUri IS NULL
    RETURN collect([act.uri, art.uri, art.datePublished]) AS articleData
    }}
    """

def orgs_by_industry_text_and_geo(industry_text, country_code, admin1_code=None):
    cache_key = cache_friendly(f"orgs_ind_text_{cacheable_hash(industry_text)}_{country_code}")
    if admin1_code is not None:
        cache_key = f"{cache_key}_{admin1_code}"
    cache_key = cache_friendly(cache_key)
    res = cache.get(cache_key)
    if res is not None:
        return res
    logger.debug(f"cache miss {cache_key}")
    res = Organization.by_industry_text_and_geo(industry_text, country_code, admin1_code)
    cache.set(cache_key, res, 60*60*4) # 4 hour cache timeout
    return res

def org_geo_industry_cluster_query_by_words(search_text: str,counts_only):
    industry_clusters = IndustryCluster.by_representative_doc_words(search_text)
    return org_geo_industry_by_clusters(industry_clusters, counts_only)

def org_geo_industry_by_clusters(industry_clusters, counts_only):    
    logger.info(f"org_geo_industry_by_clusters: Got {len(industry_clusters)} industry clusters")
    country_results = {}
    adm1_results = defaultdict(dict)
    relevant_countries = []
    relevant_admin1s = defaultdict(list)
    for ind in industry_clusters:
        logger.debug(f"org_geo_industry_by_clusters: working on {ind.topicId}")
        ind_uri = ind.uri
        country_results[ind_uri] = {}
        for cc in COUNTRY_TO_GLOBAL_REGION.keys():
            res = org_uris_by_industry_id_country_admin1(ind.topicId,cc)
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
                        res = org_uris_by_industry_id_country_admin1(ind.topicId,cc,adm1)
                        if len(res) > 0:
                            if counts_only:
                                res = len(res)
                            adm1_results[ind_uri][cc][adm1] = res
                            relevant_admin1s[cc].append(adm1)
    return industry_clusters, relevant_countries, relevant_admin1s, country_results, adm1_results

def org_geo_industry_text_by_words(search_str: str,counts_only=False):
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

        
def combined_industry_geo_results(search_str,include_search_by_industry_text=False,counts_only=True):
    logger.debug("combined_industry_geo_results started")
    ind_clusters, ind_cluster_countries, ind_cluster_admin1s_tmp, ind_cluster_by_country, ind_cluster_by_adm1 = org_geo_industry_cluster_query_by_words(search_str, counts_only)
    logger.debug("org_geo_industry_cluster_query_by_words done")
    if include_search_by_industry_text is False:
        text_countries = []
        text_admin1s_tmp = {}
        text_by_country = {}
        text_by_adm1 = {}
    else:
        text_countries, text_admin1s_tmp, text_by_country, text_by_adm1 = org_geo_industry_text_by_words(search_str, counts_only)
        logger.debug("org_geo_industry_text_by_words done")

    countries = set(ind_cluster_countries + text_countries)

    admin1s = defaultdict(set)
    for k,v in ind_cluster_admin1s_tmp.items():
        admin1s[k].update(v)    
    for k, v in text_admin1s_tmp.items():
        admin1s[k].update(v)

    country_hierarchy, country_widths, admin1_hierarchy, admin1_widths = build_region_hierarchy(countries, admin1s)
    headers = prepare_headers(country_hierarchy, country_widths, admin1_hierarchy, admin1_widths,
                              countries, admin1s)
    logger.debug("prepare_headers done")
    if len(headers) == 0:
        # No data at all
        return [], [], []
    empty_value = 0 if counts_only is True else None
    ind_cluster_rows, text_row = prepare_rows(headers[-1], ind_clusters, ind_cluster_by_country, ind_cluster_by_adm1, 
                        search_str, text_by_country, text_by_adm1, empty_value)
    logger.debug("prepare_rows done")

    return headers, ind_cluster_rows, text_row

def prepare_rows(header_row, ind_clusters, ind_cluster_by_country, ind_cluster_by_adm1, 
                        search_str, text_by_country, text_by_adm1,empty_value):
    ind_cluster_rows = []
    for ind_cluster in ind_clusters:
        ind_uri = ind_cluster.uri
        row = { "uri":ind_cluster.uri, "name": ind_cluster.longest_representative_doc,
               "industry_id":ind_cluster.topicId }
        vals = []
        all_zeros = True
        for loc in header_row:
            val, clean_loc = get_val_for_country_admin1(loc, ind_cluster_by_country[ind_uri],
                                             ind_cluster_by_adm1[ind_uri],empty_value)
            vals.append({"value":val,"region_code":clean_loc})
            if val > 0:
                all_zeros = False        
        row['vals'] = vals
        if all_zeros is False:
            ind_cluster_rows.append(row)

    text_row = { "uri":"", "name":search_str }
    vals = []
    for loc in header_row:
        val, clean_loc = get_val_for_country_admin1(loc, text_by_country,text_by_adm1,empty_value)
        vals.append({"value":val,"region_code":clean_loc})
    text_row['vals'] = vals
    return ind_cluster_rows, text_row

def get_val_for_country_admin1(loc, country_data, admin1_data, empty_value):
    search_loc = loc.replace("REPEATED","")
    search_loc = search_loc.replace("(all)","")
    search_loc = search_loc.strip()
    if "-" in search_loc:
        country, admin1 = search_loc.split("-")
        val = admin1_data.get(country,{}).get(admin1,empty_value)
    else:
        val = country_data.get(search_loc,empty_value)
    return val, search_loc

def filtered_region_classes(region_names, relevant_countries, relevant_admin1s, 
                            regions_to_countries = GLOBAL_REGION_TO_COUNTRIES_FLAT, # region1#region2
                            ):
    global_region_str = "#".join(region_names)
    country_codes = []
    for potential_country_code in regions_to_countries[global_region_str]:
        if potential_country_code not in relevant_countries:
            continue
        country_codes.append(potential_country_code)
        for admin1 in relevant_admin1s.get(potential_country_code,[]):
            country_codes.append(f"{potential_country_code}-{admin1}")
    region_classes = [f"col-{x}" for x in country_codes]
    return " ".join(sorted(region_classes))

def admin1_column_classes(country_code, admin1s):
    col_classes = [f"col-{country_code}"]
    for admin1 in admin1s.get(country_code,[]):
        col_classes.append(f"col-{country_code}-{admin1}")
    return " ".join(sorted(col_classes))
    

def get_filtered_us_region_classes(region_name, us_admin1s,
                               us_regions_to_admin1s = US_NATIONAL_REGIONS_TO_STATES, # each region is its own key
                               ):
    admin1_classes = []
    for state in us_regions_to_admin1s[region_name]:
        if state in us_admin1s:
            admin1_classes.append(f"col-US-{state}")
    return " ".join(sorted(admin1_classes))


def prepare_headers(country_hierarchy, country_widths, admin1_hierarchy, admin1_widths,
                    countries, admin1s):
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
        col_width = global_region_width( [region1], country_widths, admin1_widths)
        row1_classes = filtered_region_classes( [region1], countries, admin1s)
        row1[region1] = {"colspan":col_width, "classes":row1_classes}
        for region2, region2_vals in sorted(region1_vals.items()):
            col_width = global_region_width( [region1, region2], country_widths, admin1_widths)
            row2_classes = filtered_region_classes( [region1,region2], countries, admin1s)
            row2[region2] = {"colspan":col_width,"classes":row2_classes}
            if isinstance(region2_vals, dict):
                # There is a row3
                for region3, region3_vals in sorted(region2_vals.items()):
                    col_width = global_region_width( [region1,region2, region3], country_widths, admin1_widths)
                    row3_classes = filtered_region_classes( [region1, region2, region3], countries, admin1s)
                    row3[region3] = {"colspan":col_width, "classes":row3_classes}
                    for country in sorted(region3_vals):
                        col_width = admin1_width_for_country(country, admin1_widths)
                        row4_classes = admin1_column_classes(country, admin1s)
                        row4[country] = {"colspan":col_width, "classes": row4_classes}
                        country_only_class = f"col-{country}"
                        if col_width == 1:
                            row5[f"REPEATED {country}"] = {"colspan":1, "classes": country_only_class}
                            row6[f"REPEATED {country}"] = {"colspan":1, "classes": country_only_class}
                            row7[f"REPEATED {country}"] = {"colspan":1, "classes": country_only_class}
                            continue
                        row5[f"{country} (all)"] = {"colspan":1, "classes": country_only_class}
                        row6[f"REPEATED {country} (all)"] = {"colspan":1, "classes": country_only_class}
                        row7[f"REPEATED {country} (all)"] = {"colspan":1, "classes": country_only_class}
                        if country == "US":
                            add_us_regions(row5,row6,row7,
                                           admin1_hierarchy.get("US",{}),
                                           admin1.get("US",[]),admin1_widths)
                        else:
                            for admin1 in admin1_hierarchy[country]:
                                row5[f"{country-admin1}"] = {"colspan":1,"classes":f"col-{country}-{admin1}"}
                                row6[f"REPEATED {country-admin1}"] = {"colspan":1,"classes":f"col-{country}-{admin1}"}
                                row7[f"REPEATED {country}-{admin1}"] = {"colspan":1,"classes":f"col-{country}-{admin1}"}
            else:
                row3[f"REPEATED {region2}"] = {"colspan":col_width, "classes":row2_classes}
                for country in sorted(region2_vals):
                    col_width = admin1_width_for_country(country, admin1_widths)
                    row4_classes = admin1_column_classes(country, admin1s)
                    row4[country] = {"colspan":col_width, "classes":row4_classes}
                    country_classes = f"col-{country}"
                    if col_width == 1:
                        row5[f"REPEATED {country}"] = {"colspan":1, "classes":country_classes}
                        row6[f"REPEATED {country}"] = {"colspan":1, "classes":country_classes}
                        row7[f"REPEATED {country}"] = {"colspan":1, "classes":country_classes}
                        continue
                    row5[f"{country} (all)"] = {"colspan":1, "classes":country_classes}
                    row6[f"REPEATED {country} (all)"] = {"colspan":1, "classes":country_classes}
                    row7[f"REPEATED {country} (all)"] = {"colspan":1, "classes":country_classes}
                    if country == "US":
                        add_us_regions(row5,row6,row7,admin1_hierarchy["US"],admin1s.get("US",[]),admin1_widths)
                    else:
                        for admin1 in admin1_hierarchy[country]:
                            row5[f"{country}-{admin1}"] = {"colspan":1,"classes":f"col-{country}-{admin1}"}
                            row6[f"REPEATED {country}-{admin1}"] = {"colspan":1,"classes":f"col-{country}-{admin1}"}
                            row7[f"REPEATED {country}-{admin1}"] = {"colspan":1,"classes":f"col-{country}-{admin1}"}

    if row_has_relevant_content(row1):
        results.append(row1)   
    if row_has_relevant_content(row2):
        results.append(row2)
    if row_has_relevant_content(row3):
        results.append(row3)
    if row_has_relevant_content(row4):
        results.append(row4)
    if row_has_relevant_content(row5):
        results.append(row5)
    if row_has_relevant_content(row6):
        results.append(row6)
    if row_has_relevant_content(row7):
        results.append(row7)

    for val in results[-1].values():
        val["classes"] = val["classes"] + " header_final"
    return results

def add_us_regions(row5,row6,row7,us_admin1_data,us_admin1s,admin1_widths):
    for us_region,vals in sorted(us_admin1_data.items()):
        col_width = admin1_widths["US"][f"US#{us_region}"]
        classes = get_filtered_us_region_classes(us_region, us_admin1s)
        row5[us_region] = {"colspan":col_width, "classes":classes}
        for us_division, states in sorted(vals.items()):
            col_width = admin1_widths["US"][f"US#{us_region}#{us_division}"]
            classes = get_filtered_us_region_classes(us_division, us_admin1s)
            row6[us_division] = {"colspan":col_width, "classes":classes}
            for state in states:
                row7[f"US-{state}"] = {"colspan":1, "classes":f"col-US-{state}"}

def row_has_relevant_content(row):
    relevant_content = any( [ not(x.startswith("REPEATED")) for x in row.keys()])
    return relevant_content
    
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
            us_hierarchy = filtered_hierarchy(US_REGIONS_TO_STATES_HIERARCHY,provinces)
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
    
