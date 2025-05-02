from neomodel import db
from topics.models import *
from typing import Dict
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

def search_organizations_by_name(name, combine_same_as_name_only=True, limit=20):
    res = search_by_name(name)
    if combine_same_as_name_only is True:
        res = remove_same_as_name_onlies(res)
    sorted_res = sorted(res, key=lambda x: x[1], reverse=True)
    return sorted_res[:limit]


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
    
def get_same_as_name_onlies(org):
    try:
        clean_names = (org.internalCleanName or []) + (org.internalCleanShortName or [])
        if len(clean_names) == 0:
            logger.debug("Nothing to search for")
            return []
        vals = []
        for clean_name in clean_names:
            new_vals = get_by_internal_clean_name(clean_name)
            for k in new_vals.keys():
                if k != org:
                    vals.append(k)
        return vals
    except AttributeError as ae:
        logger.debug(f"{ae} : not looking for any same names")
        return []
    
def get_by_internal_clean_name(clean_name: str) -> Dict:
    cache_key = cache_friendly(f"sano_{clean_name}") # sano = same_as_name_only
    res = cache.get(cache_key)
    if res is not None:
        logger.info(f"cache hit with {cache_key}")
        return res
    equivalent_orgs_for_this_clean_name = defaultdict(int)
    search_results_1 = do_search_by_clean_name(clean_name, "organization_clean_name")
    search_results_2 = do_search_by_clean_name(clean_name, "organization_clean_short_name")
    update_equivalent_orgs(equivalent_orgs_for_this_clean_name, search_results_1)
    update_equivalent_orgs(equivalent_orgs_for_this_clean_name, search_results_2)
    equivalent_orgs_for_this_clean_name = {k: v for k, v in sorted(equivalent_orgs_for_this_clean_name.items(), key=lambda x: x[1], reverse=True)}
    cache.set(cache_key, equivalent_orgs_for_this_clean_name)
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
    query = f"""CALL db.index.fulltext.queryNodes($index_name, $repeated_clean_name, {{analyzer:"keyword"}}) YIELD node, score
            WHERE $clean_name IN node.{attribute}
            RETURN node"""   
    logger.debug(query)
    repeated_clean_name = f"{clean_name} OR {clean_name}" # yes, weird huh.
    vals, _ = db.cypher_query(query,params={"clean_name":clean_name,
                                            "index_name":index_name,
                                            "repeated_clean_name":repeated_clean_name}, resolve_objects=True)
    merged_node_uris = set()
    for row in vals:
        merged_node = Resource.self_or_ultimate_target_node(row[0])
        merged_node_uris.add(merged_node.uri)
    query2 = f"""MATCH (n: Resource) WHERE n.uri IN {list(merged_node_uris)}
                RETURN n, apoc.node.degree(n)"""
    vals, _ = db.cypher_query(query2,resolve_objects=True)
    return vals

def search_by_name(name):
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
    to_keep = defaultdict(int)
    found_names = []
    for org, count in reference_org_list:
        logger.info(f"Checking {org.uri} - {org.best_name}")
        clean_names = org.internalCleanName or []
        clean_short_names = org.internalCleanShortName or []
        if any(x in found_names for x in org.name + 
                                clean_names + clean_short_names):
            logger.info(f"Already found, skipping")
            continue
        found_names.extend(org.name)
        found_names.extend(clean_names)
        found_names.extend(clean_short_names)
        to_keep[org] += count
    logger.info(f"Got {len(to_keep)} items")
    return list(to_keep.items())
