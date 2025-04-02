from neomodel import db
from topics.models import *
from typing import List
import logging
logger = logging.getLogger(__name__)

def search_organizations_by_name(name, combine_same_as_name_only=True, limit=20):
    res = search_by_name(name)
    if combine_same_as_name_only is True:
        res = remove_same_as_name_onlies(res)
    return res[:limit]

def random_org_list(limit=10):
    query = f"""MATCH (n: Resource&Organization)
            WHERE n.internalMergedSameAsHighToUri IS NULL
            AND SIZE(LABELS(n)) = 2
            WITH n, rand() as r
            WHERE r < 0.01
            WITH n LIMIT {limit * 10}
            RETURN n, apoc.node.degree(n) as relationship_count"""
    vals, _ = db.cypher_query(query, resolve_objects=True)
    cleaned = remove_same_as_name_onlies(vals)
    return cleaned[:limit]
    
def get_same_as_name_onlies(org):
    try:
        clean_names = (org.internalCleanName or []) + (org.internalCleanShortName or [])
        if len(clean_names) == 0:
            logger.debug("Nothing to search for")
            return []
        res = search_by_internal_clean_names(clean_names)
        vals = set()
        for x in res:
            node = Resource.self_or_ultimate_target_node(x[0])
            if node.uri != org.uri:
                vals.add(node)
        return vals
    except AttributeError as ae:
        logger.warning(f"{ae} : not looking for any same names")
        return []

def search_by_internal_clean_names(clean_names: List[str]):
    if len(clean_names) == 1:
        clean_names = clean_names + clean_names
    query1 = """WITH $clean_names as terms
            CALL db.index.fulltext.queryNodes("organization_clean_name", apoc.text.join(terms, " OR "),{analyzer:"keyword"}) YIELD node, score
            WHERE ANY(term IN node.internalCleanName WHERE term IN terms)
            RETURN node"""
    res1,_ = db.cypher_query(query1,params={"clean_names":clean_names},resolve_objects=True)
    query2 = """WITH $clean_names as terms
            CALL db.index.fulltext.queryNodes("organization_clean_short_name", apoc.text.join(terms, " OR "),{analyzer:"keyword"}) YIELD node, score
            WHERE ANY(term IN node.internalCleanShortName WHERE term IN terms)
            RETURN node"""
    res2,_ = db.cypher_query(query2,params={"clean_names":clean_names},resolve_objects=True)
    return res1 + res2


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
    Each row is tuple of Org and number of relationships
    '''
    to_keep = []
    found_names = []
    for org, count in reference_org_list:
        clean_names = org.internalCleanName or []
        clean_short_names = org.internalCleanShortName or []
        if any(x in found_names for x in org.name + 
                                clean_names + clean_short_names):
            continue
        found_names.extend(org.name)
        found_names.extend(clean_names)
        found_names.extend(clean_short_names)
        to_keep.append([org, count])
    return to_keep
               
