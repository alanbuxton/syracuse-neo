from neomodel import db
from topics.models import *
from typing import Dict, List, Tuple, Union
import logging
from collections import defaultdict
from topics.util import clean_punct, standardize_name
from syracuse.cache_util import get_versionable_cache, set_versionable_cache

logger = logging.getLogger(__name__)

def search_organizations_by_name(name, combine_same_as_name_only=True, 
                                 top_1_strict=False, limit: int = 20) -> List[Tuple[Organization, int]]:
    name = standardize_name(name)
    res = search_by_name(name)
    if combine_same_as_name_only is True:
        res = remove_same_as_name_onlies(res)
    sorted_res = sorted(res, key=lambda x: x[1], reverse=True)
    if top_1_strict is True:
        return top_1_strict_search(name, sorted_res)
    return sorted_res[:limit]

def top_1_strict_search(name: str, results: List) -> List[Tuple[Organization, int]]:
    name = name.replace(" ","")
    for org,vals in results:
        for target_org_name in [standardize_name(x).replace(" ","") for x in org.name]:
            if target_org_name.startswith(name):
                return [(org, vals)]
    return []
    
def random_org_list(limit=10):
    query = f"""MATCH (n: Resource&Organization)
            WHERE n.internalMergedSameAsHighToUri IS NULL
            AND SIZE(LABELS(n)) = 2
            WITH n, rand() as r
            WHERE r < 0.01
            WITH n LIMIT {limit * 10}
            RETURN n, apoc.node.degree(n) as relationship_count
            ORDER BY relationship_count DESC"""
    vals, _ = db.cypher_query(query, resolve_objects=True)
    cleaned = remove_same_as_name_onlies(vals)
    return cleaned[:limit]
    
def get_same_as_name_onlies(org, version=None):
    try:
        clean_names = (org.internalCleanName or []) + (org.internalCleanShortName or [])
        if len(clean_names) == 0:
            logger.debug("Nothing to search for")
            return []
        vals = []
        for clean_name in clean_names:
            new_vals = get_by_internal_clean_name(clean_name, version)
            for k in new_vals.keys():
                if k != org:
                    vals.append(k)
        return vals
    except AttributeError as ae:
        logger.debug(f"{ae} : not looking for any same names")
        return []
    
def get_by_internal_clean_name(clean_name: str, version=None) -> Dict:
    logger.debug(f"get_by_internal_clean_name {clean_name}")
    cache_key = f"sano_{clean_name}" # sano = same_as_name_only
    res = get_versionable_cache(cache_key, version)
    if res is not None:
        logger.debug(f"get_by_internal_clean_name: cache hit with {cache_key}")
        return res
    logger.debug(f"get_by_internal_clean_name: cache miss with {cache_key}")
    equivalent_orgs_for_this_clean_name = defaultdict(int)
    search_results_1 = do_search_by_clean_name(clean_name, "organization_clean_name")
    search_results_2 = do_search_by_clean_name(clean_name, "organization_clean_short_name")
    update_equivalent_orgs(equivalent_orgs_for_this_clean_name, search_results_1)
    update_equivalent_orgs(equivalent_orgs_for_this_clean_name, search_results_2)
    equivalent_orgs_for_this_clean_name = {k: v for k, v in sorted(equivalent_orgs_for_this_clean_name.items(), key=lambda x: x[1], reverse=True)}
    set_versionable_cache(cache_key, equivalent_orgs_for_this_clean_name, version)
    return equivalent_orgs_for_this_clean_name

def update_equivalent_orgs(equivalent_orgs_for_this_clean_name, search_results):
    for row in search_results:
        uri = row[0]
        rel_count = row[1]
        equivalent_orgs_for_this_clean_name[uri] += rel_count

def do_search_by_clean_name(clean_name, index_name):
    if index_name == 'organization_clean_name':
        attribute = 'internalCleanName'
    elif index_name == 'organization_clean_short_name':
        attribute = 'internalCleanShortName'
    else:
        raise ValueError(f"Don't know how to handle index {index_name}")
    query = f"""CALL db.index.fulltext.queryNodes($index_name, $clean_name_no_punct) YIELD node, score
            WHERE $clean_name IN node.{attribute}
            RETURN node"""   
    logger.debug(query)
    clean_name_no_punct = clean_punct(clean_name)
    vals, _ = db.cypher_query(query,params={"clean_name":clean_name,
                                            "index_name":index_name,
                                            "clean_name_no_punct":clean_name_no_punct}, resolve_objects=True)
    merged_node_uris = set()
    for row in vals:
        merged_node = Resource.self_or_ultimate_target_node(row[0])
        merged_node_uris.add(merged_node.uri)
    query2 = f"""MATCH (n: Resource) WHERE n.uri IN {list(merged_node_uris)}
                RETURN n, apoc.node.degree(n)"""
    vals, _ = db.cypher_query(query2,resolve_objects=True)
    return vals

def search_by_name(name) -> list:
    '''
        Returns tuple of Organization, Count of Relationships
    '''
    query = f'''CALL db.index.fulltext.queryNodes("resource_names", "{name}",{{ analyzer: "classic"}}) YIELD node as n
        WITH n
        WHERE "Organization" IN LABELS(n)
        AND SIZE(LABELS(n)) = 2
        AND n.internalMergedSameAsHighToUri IS NULL
        RETURN n, apoc.node.degree(n) AS relationship_count
        ORDER BY relationship_count DESCENDING;'''
    vals, _ = db.cypher_query(query,resolve_objects=True)
    return vals # List of items and number of relationships

def remove_same_as_name_onlies(reference_org_list):
    '''
    Each row is tuple of Org and number of relationships,
    Return is the same
    '''
    logger.debug(f"remove_same_as_name_onlies {len(reference_org_list)}")
    to_keep = defaultdict(int)
    found_names = []
    for org, count in reference_org_list:
        logger.debug(f"Checking {org.uri} - {org.best_name}")
        clean_names = org.internalCleanName or []
        clean_short_names = org.internalCleanShortName or []
        if any(x in found_names for x in org.name + 
                                clean_names + clean_short_names):
            logger.debug(f"Already found, skipping")
            continue
        found_names.extend(org.name)
        found_names.extend(clean_names)
        found_names.extend(clean_short_names)
        to_keep[org] += count
    logger.debug(f"Got {len(to_keep)} items")
    return list(to_keep.items())
