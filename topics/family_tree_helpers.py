from typing import Union, List
from .models import Article, Organization
from .constants import BEGINNING_OF_TIME
from neomodel import db
import logging
from .graph_utils import keep_or_switch_node
from .neo4j_utils import date_to_cypher_friendly

logger = logging.getLogger(__name__)


def do_get_parent_orgs_query(uri_or_uris: Union[str,List], parent_rels = "buyer|vendor|investor",
                             source_names=Article.core_sources(),
                             min_date=BEGINNING_OF_TIME):
    '''
        Returns parent, child, Activity, Article, type of relationship (str), doc extract (str)
    '''
    if isinstance(uri_or_uris, str):
        uri_or_uris = [uri_or_uris]
    else:
        uri_or_uris = list(uri_or_uris)
    assert all(["'" not in x for x in uri_or_uris]), f"Can't have ' in uri: {uri_or_uris}"
    query = f"""
        MATCH (a: Article)<-[d:documentSource]-(c: CorporateFinanceActivity)-[:target]->(t: Resource&Organization),
        (b: Resource&Organization)-[x:{parent_rels}]-(c: CorporateFinanceActivity)
        WHERE t.uri in {uri_or_uris}
        AND b.internalMergedSameAsHighToUri IS NULL
        AND t.internalMergedSameAsHighToUri IS NULL
        AND a.sourceOrganization in {source_names}
        AND a.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        RETURN b, t, c, a, TYPE(x), d.documentExtract
        ORDER BY a.datePublished DESCENDING
    """
    results, _ = db.cypher_query(query, resolve_objects=True)
    logger.debug(query)
    return results

def do_get_child_orgs_query(uri_or_uris: Union[str,List], relationships = "buyer|vendor|investor",
                            source_names=Article.core_sources(),
                            min_date=BEGINNING_OF_TIME):
    if isinstance(uri_or_uris, str):
        uri_or_uris = [uri_or_uris]
    else:
        uri_or_uris = list(uri_or_uris)
    assert all(["'" not in x for x in uri_or_uris]), f"Can't have ' in uri: {uri_or_uris}"
    query = f"""
        MATCH (a: Article)<-[d:documentSource]-(c: CorporateFinanceActivity)-[:target]->(t: Resource&Organization),
        (b: Resource&Organization)-[x:{relationships}]-(c: CorporateFinanceActivity)
        WHERE b.uri in {uri_or_uris}
        AND t.internalMergedSameAsHighToUri IS NULL
        AND b.internalMergedSameAsHighToUri IS NULL
        AND a.sourceOrganization in {source_names}
        AND a.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        RETURN b, t, c, a, TYPE(x), d.documentExtract
        ORDER BY a.datePublished DESCENDING
    """
    logger.debug(query)
    results, _ = db.cypher_query(query, resolve_objects=True)
    return results

def get_child_orgs(uri, combine_same_as_name_only=True, relationships="buyer|vendor|investor",
                   nodes_found_so_far=set(), source_names=Article.core_sources(),
                   earliest_doc_date=BEGINNING_OF_TIME,
                   override_target_uri=None):
    logger.debug(f"get_child_orgs for {uri}")
    if combine_same_as_name_only is False:
        res = do_get_child_orgs_query(uri, relationships, source_names, earliest_doc_date)
        return res
    org = Organization.self_or_ultimate_target_node(uri)
    if override_target_uri:
        override_target_org = Organization.self_or_ultimate_target_node(override_target_uri)
    same_as_uris = [x.uri for x in org.sameAsNameOnly if x.internalMergedSameAsHighToUri is None]
    res = do_get_child_orgs_query(same_as_uris + [uri], relationships, source_names, earliest_doc_date)
    items_to_keep = []
    for item in res:
        target_node_or_same_as = keep_or_switch_node(item[1], nodes_found_so_far, combine_same_as_name_only)
        if target_node_or_same_as == item[1]:
            item[0] = keep_or_switch_node(item[0], nodes_found_so_far, combine_same_as_name_only)
            if override_target_uri and override_target_org in item[1].sameAsNameOnly:
                item[1] = override_target_org
            items_to_keep.append(item)
    return items_to_keep

def get_parent_orgs(uri, combine_same_as_name_only=True, relationships="buyer|vendor|investor",
                    nodes_found_so_far=set(),source_names=Article.core_sources(),
                    earliest_doc_date=BEGINNING_OF_TIME):
    logger.debug(f"get_parent_orgs for {uri}")
    if combine_same_as_name_only is False:
        res = do_get_parent_orgs_query(uri, relationships, source_names, earliest_doc_date)
        return res

    org = Organization.self_or_ultimate_target_node(uri)
    same_as_uris = [x.uri for x in org.sameAsNameOnly if x.internalMergedSameAsHighToUri is None]
    res = do_get_parent_orgs_query(same_as_uris + [uri], relationships, source_names, earliest_doc_date)
    items_to_keep = []
    for item in res:
        node_or_same_as = keep_or_switch_node(item[0], nodes_found_so_far, combine_same_as_name_only)
        if node_or_same_as == item[0]:
            item[1] = org # the child might be via a sameAsNameOnly
            items_to_keep.append(item)
    return items_to_keep

def org_family_tree(organization_uri, combine_same_as_name_only=True, relationships="buyer|vendor|investor",
                    source_names=Article.core_sources(),
                    earliest_doc_date=BEGINNING_OF_TIME):
    logger.debug(f"org_family_tree for {organization_uri} with '{relationships}' and '{source_names}'")
    nodes_found_so_far = set()
    children = get_child_orgs(organization_uri,
                    combine_same_as_name_only=combine_same_as_name_only,
                    relationships=relationships,nodes_found_so_far=nodes_found_so_far,
                    source_names=source_names,earliest_doc_date=earliest_doc_date)
    parents = get_parent_orgs(organization_uri,
                    combine_same_as_name_only=combine_same_as_name_only,
                    relationships=relationships,nodes_found_so_far=nodes_found_so_far,
                    source_names=source_names,earliest_doc_date=earliest_doc_date)

    siblings = []
    for org,_,_,_,_,_ in parents:
        siblings.extend(get_child_orgs(org.uri,
                        combine_same_as_name_only=combine_same_as_name_only,
                        relationships=relationships,nodes_found_so_far=nodes_found_so_far,
                        source_names=source_names,earliest_doc_date=earliest_doc_date,
                        override_target_uri=organization_uri))

    return parents, siblings, children