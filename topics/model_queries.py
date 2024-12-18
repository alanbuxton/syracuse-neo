from neomodel import db
from .models import (Organization, ActivityMixin, Article, 
                     date_to_cypher_friendly, IndustryCluster
)
from .geo_utils import geoname_ids_for_country_region, geo_select_list
from datetime import datetime, timezone, timedelta
from typing import List, Union
import logging
from precalculator.models import P
from .graph_utils import keep_or_switch_node
from .constants import BEGINNING_OF_TIME

logger = logging.getLogger(__name__)

def get_activities_for_serializer_by_country_and_date_range(geo_code,min_date,max_date,limit=20,combine_same_as_name_only=True):
    relevant_uris = get_relevant_org_uris_for_country_region_industry(geo_code,limit=None)
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, uri_or_list=relevant_uris,
                                max_date=max_date, limit=limit, combine_same_as_name_only=combine_same_as_name_only)
    return matching_activity_orgs

def get_activities_for_serializer_by_source_and_date_range(source_name, min_date, max_date, limit=20):
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, source_name=source_name,
                                max_date=max_date, limit=limit, combine_same_as_name_only=False)
    return matching_activity_orgs

def get_relevant_org_uris_for_country_region_industry(geo_code, industry_id=None, limit=20):
    uris=Organization.by_industry_and_or_geo(industry_id, geo_code,limit=limit, uris_only=True)
    return uris

def get_activities_for_serializer_by_industry_and_date_range(industry, min_date, max_date, limit=20):
    matching_activity_orgs = do_get_activities_articles_by_industry_and_date_range_query(industry, min_date=min_date,
                                max_date=max_date, limit=limit)
    return activity_articles_to_api_results(matching_activity_orgs)

def get_activities_by_date_range_for_api(min_date, uri_or_list: Union[str,List[str],None] = None,
                                            source_name: Union[str,None] = None,
                                            max_date = datetime.now(tz=timezone.utc),
                                            limit = None, combine_same_as_name_only=True):
    assert min_date is not None, "Must have min date"
    assert min_date <= max_date,  f"Min date {min_date} must be before or same as max date {max_date}"
    if (uri_or_list is None or len(uri_or_list) == 0 or set(uri_or_list) == {None}) and source_name is None:
        return []
    if source_name is None:
        activity_articles = get_activities_by_org_uri_and_date_range(uri_or_list, min_date, max_date, limit,combine_same_as_name_only)
    else:
        activity_articles = get_activities_by_source_and_date_range(source_name, min_date, max_date, limit, combine_same_as_name_only)
    return activity_articles_to_api_results(activity_articles)

def get_activities_by_date_range_industry_geo_for_api(min_date, max_date,geo_code,industry_id, limit=None):
    if industry_id is None:
        allowed_org_uris = None
    else:
        allowed_org_uris = Organization.by_industry_and_or_geo(industry_id,
                            geo_code,uris_only=True,limit=None,allowed_to_set_cache=True)
    geonames_ids = geoname_ids_for_country_region(geo_code)
    query = build_get_activities_by_date_range_industry_geo_query(min_date, max_date, allowed_org_uris, geonames_ids, limit)
    objs, _ = db.cypher_query(query, resolve_objects=True)
    return activity_articles_to_api_results(objs)

def build_get_activities_by_date_range_industry_geo_query(min_date, max_date, allowed_org_uris, 
                                                          geonames_ids, limit=None, counts_only=False):
    if geonames_ids is None:
        geo_clause = ''
    else:
        geo_clause = f"""AND (EXISTS {{ MATCH (x)-[:whereGeoNamesLocation]-(loc:GeoNamesLocation) WHERE loc.geoNamesId IN {list(geonames_ids)} }}
                        OR EXISTS {{ MATCH (o)-[:basedInHighGeoNamesLocation]-(loc:GeoNamesLocation) WHERE loc.geoNamesId in {list(geonames_ids)} }})"""
    if allowed_org_uris is None:
        org_uri_clause = ''
    else:
        org_uri_clause = f"AND o.uri IN {allowed_org_uris}"
    if limit is None:
        limit_clause = ''
    else:
        limit_clause = f' LIMIT {limit}'
    query = f"""
        MATCH (a: Article)<-[:documentSource]-(x: CorporateFinanceActivity|LocationActivity|PartnershipActivity)--(o: Resource&Organization)
        WHERE a.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND a.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
        AND o.internalMergedSameAsHighToUri IS NULL
        {org_uri_clause}
        {geo_clause}
        RETURN x,a
        {limit_clause}
        UNION
        MATCH (a: Article)<-[:documentSource]-(x: RoleActivity)--(p: Role)--(o: Resource&Organization)
        WHERE a.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND a.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
        AND o.internalMergedSameAsHighToUri IS NULL
        {org_uri_clause}
        {geo_clause}
        RETURN x,a
        {limit_clause}
    """
    logger.debug(query)
    return query

def activity_articles_to_api_results(activity_articles):
    api_results = []
    for activity,article in activity_articles:
        assert isinstance(activity, ActivityMixin), f"{activity} should be an Activity"
        api_row = {}
        api_row["source_organization"] = article.sourceOrganization
        api_row["date_published"] = article.datePublished
        api_row["headline"] = article.headline
        api_row["document_extract"] = activity.documentSource.relationship(article).documentExtract
        api_row["document_url"] = article.documentURL
        api_row["archive_org_page_url"] = article.archiveOrgPageURL
        api_row["archive_org_list_url"] = article.archiveOrgListURL
        api_row["activity_uri"] = activity.uri
        api_row["activity_locations"] = activity.whereGeoNamesLocation
        api_row["activity_location_as_string"] = activity.whereGeoName_as_str
        api_row["activity_class"] = activity.__class__.__name__
        api_row["activity_types"] = activity.activityType
        api_row["activity_longest_type"] = activity.longest_activityType
        api_row["activity_statuses"] = activity.status
        api_row["activity_status_as_string"] = activity.status_as_string
        api_row["source_is_core"] = article.is_core
        actors = {}
        for actor_role, actor in activity.all_actors.items():
            if actor is not None and actor != []:
                if actors.get(actor_role) is None:
                    actors[actor_role] = set()
                actors[actor_role].update(actor)
        api_row["actors"] = actors
        api_results.append(api_row)
    return api_results

def get_all_source_names():
    sources, _ = db.cypher_query("MATCH (n:Article) RETURN DISTINCT n.sourceOrganization;")
    flattened = [x for sublist in sources for x in sublist]
    return flattened

def get_activities_by_source_and_date_range(source_name,min_date, max_date, limit=None,counts_only=False):
    query = build_get_activities_by_source_and_date_range_query(source_name,min_date, max_date, limit,counts_only)
    objs, _ = db.cypher_query(query, resolve_objects=True)
    return objs[:limit]

def build_get_activities_by_source_and_date_range_query(source_name,min_date, max_date, limit,counts_only):
    if counts_only is True:
        return_str = "RETURN COUNT(DISTINCT(n))"
    else:
        return_str = "RETURN n,a ORDER BY a.publishDate DESC"
    if limit is not None:
        limit_str = f"LIMIT {limit}"
    else:
        limit_str = ""
    where_clause = f"""WHERE a.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
                    AND a.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
                    AND a.sourceOrganization = ('{source_name}')
                    AND n.internalMergedSameAsHighToUri IS NULL"""

    query = f"""MATCH (n:CorporateFinanceActivity|LocationActivity|PartnershipActivity)-[:documentSource]->(a:Article)
                {where_clause}
                {return_str} {limit_str}
                UNION
                MATCH (a: Article)<-[:documentSource]-(n:RoleActivity)--(Role)--(Organization)
                {where_clause}
                {return_str} {limit_str};"""
    return query

def get_activities_by_org_uri_and_date_range(uri_or_uri_list: Union[str,List], min_date,
                        max_date, limit=None, combine_same_as_name_only=True, counts_only = False):
    query=build_get_activities_by_org_uri_and_date_range_query(uri_or_uri_list,
                        min_date, max_date, limit=limit, combine_same_as_name_only=combine_same_as_name_only,
                        counts_only = counts_only)
    objs, _ = db.cypher_query(query, resolve_objects=True)
    return objs[:limit]

def build_get_activities_by_org_uri_and_date_range_query(uri_or_uri_list: Union[str,List],
                    min_date, max_date, limit=None, combine_same_as_name_only=True,
                    counts_only = False):
    if isinstance(uri_or_uri_list, str):
        uri_list = [uri_or_uri_list]
    elif isinstance(uri_or_uri_list, set):
        uri_list = list(uri_or_uri_list)
    else:
        uri_list = uri_or_uri_list
    orgs = Organization.nodes.filter(uri__in=uri_list)
    uris_to_check = set(uri_list)
    if combine_same_as_name_only is True:
        for org in orgs:
            new_uris = [x.uri for x in org.sameAsNameOnly]
            uris_to_check.update(new_uris)
    if limit is not None:
        limit_str = f"LIMIT {limit}"
    else:
        limit_str = ""
    if counts_only is True:
        return_str = "RETURN COUNT(DISTINCT(n))"
    else:
        return_str = "RETURN n,a ORDER BY a.publishDate DESC"
    where_clause = f"""WHERE a.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
                        AND a.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
                        AND (o.uri IN {list(uris_to_check)})
                        AND n.internalMergedSameAsHighToUri IS NULL
                    """
    query = f"""
        MATCH (a: Article)<-[:documentSource]-(n:CorporateFinanceActivity|LocationActivity|PartnershipActivity)--(o: Resource&Organization)
        {where_clause}
        {return_str} {limit_str}
        UNION
        MATCH (a: Article)<-[:documentSource]-(n:RoleActivity)--(p: Role)--(o: Resource&Organization)
        {where_clause}
        {return_str} {limit_str};
    """
    logger.debug(query)
    return query

def get_cached_stats():
    latest_date = P.get_last_updated()
    if latest_date is None:
        return None, None, None, None, None
    d = datetime.date(latest_date)
    counts, recents_by_country_region, recents_by_source, recents_by_industry = get_stats(d, allowed_to_set_cache=False)
    return d, counts, recents_by_country_region, recents_by_source, recents_by_industry

def get_stats(max_date,allowed_to_set_cache=False):
    res = P.get_stats(max_date)
    if res is not None:
        return res
    counts = []
    for x in ["Organization","Person","CorporateFinanceActivity","RoleActivity","LocationActivity","PartnershipActivity","Article","Role"]:
        res, _ = db.cypher_query(f"""MATCH (n:{x}) WHERE
                    n.internalMergedSameAsHighToUri IS NULL
                    RETURN COUNT(n)""")
        counts.append( (x , res[0][0]) )
    recents_by_country_region = []
    ts1 = datetime.now(tz=timezone.utc)
    recents_by_industry = []
    for industry in sorted(IndustryCluster.leaf_nodes_only(),
                           key=lambda x: x.longest_representative_doc):
        logger.info(f"Stats for {industry.longest_representative_doc}")
        cnt7 = counts_by_timedelta(7,max_date,industry=industry)
        cnt30 = counts_by_timedelta(30,max_date,industry=industry)
        cnt90 = counts_by_timedelta(90,max_date,industry=industry)
        if cnt7 > 0 or cnt30 > 0 or cnt90 > 0:
            recents_by_industry.append( 
                (industry.topicId, industry.longest_representative_doc,cnt7,cnt30,cnt90) )
    for k,v in geo_select_list():
        logger.info(f"Stats for {k}")
        if k.strip() == '':
            continue
        cnt7 = counts_by_timedelta(7,max_date,geo_code=k)
        cnt30 = counts_by_timedelta(30,max_date,geo_code=k)
        cnt90 = counts_by_timedelta(90,max_date,geo_code=k)
        if cnt7 > 0 or cnt30 > 0 or cnt90 > 0:
            country_code = k[:2]
            recents_by_country_region.append( (country_code,k,v,cnt7,cnt30,cnt90) )
    recents_by_source = []
    for source_name in sorted(get_all_source_names()):
        logger.info(f"Stats for {source_name}")
        cnt7 = counts_by_timedelta(7,max_date,source_name=source_name)
        cnt30 = counts_by_timedelta(30,max_date,source_name=source_name)
        cnt90 = counts_by_timedelta(90,max_date,source_name=source_name)
        if cnt7 > 0 or cnt30 > 0 or cnt90 > 0:
            recents_by_source.append( (source_name,cnt7,cnt30,cnt90) )
    ts2 = datetime.now(tz=timezone.utc)
    logger.info(f"counts_by_timedelta up to {max_date}: {ts2 - ts1}")
    if allowed_to_set_cache is True:
        P.set_stats(max_date, (counts, recents_by_country_region, recents_by_source, recents_by_industry) )
    else:
        logger.debug("Not allowed to set cache")
    return counts, recents_by_country_region, recents_by_source, recents_by_industry

def counts_by_timedelta(days_ago, max_date, geo_code=None,source_name=None, industry=None):
    min_date = max_date - timedelta(days=days_ago)
    if geo_code is not None:
        res = get_country_region_counts(geo_code,min_date,max_date)
    elif source_name is not None:
        res = get_source_counts(source_name,min_date,max_date)
    elif industry is not None:
        res = get_industry_counts(industry,min_date,max_date)
    else:
        raise ValueError(f"Not a valid breakdown to get stats for")
    return res

def get_source_counts(source_name, min_date, max_date):
    counts = get_activities_by_source_and_date_range(source_name,min_date,max_date,counts_only=True)
    return count_entries(counts)

def get_industry_counts(industry, min_date, max_date):
    counts = do_get_activities_articles_by_industry_and_date_range_query(industry, min_date, max_date, limit=None, counts_only=True)
    return counts

def get_country_region_counts(geo_code,min_date,max_date):
    relevant_uris = get_relevant_org_uris_for_country_region_industry(geo_code,limit=None)
    counts = get_activities_by_org_uri_and_date_range(relevant_uris,min_date,max_date,combine_same_as_name_only=False,counts_only=True)
    return count_entries(counts)

def count_entries(results):
    '''
        Expecting two results. For some reason, the union of two counts only returns one value if both are the same
    '''
    val = results[0][0]
    if len(results) == 1:
        val = val * 2
    else:
        val = val + results[1][0]
    return val

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


def do_get_activities_articles_by_industry_and_date_range_query(industry, min_date, max_date, limit=None, counts_only=False):
    industry_uri = industry.uri
    if counts_only is True:
        return_clause = "RETURN distinct(act.uri)"
    else:
        return_clause = "RETURN act, art"
    if limit is not None:
        limit_clause = f" LIMIT {limit} "
    else:
        limit_clause = ""
    query = f'''MATCH (n: IndustryCluster {{uri:"{industry_uri}"}})--(act: CorporateFinanceActivity)--(art: Article)
            WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
            AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
            {return_clause}
            {limit_clause}
            UNION
            MATCH (n: IndustryCluster {{uri:"{industry_uri}"}})--(c: Organization)--(art: Article)--(act: CorporateFinanceActivity|LocationActivity|PartnershipActivity)
            WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
            AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
            {return_clause}
            {limit_clause}
            UNION
            MATCH (n: IndustryCluster {{uri:"{industry_uri}"}})--(o: Organization)--(r: Role)--(act: RoleActivity)--(art: Article)
            WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
            AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
            {return_clause}
            {limit_clause}
            '''
    res, _ = db.cypher_query(query, resolve_objects=True)
    if counts_only is True:
        flattened = [x for sublist in res for x in sublist]
        return len(set(flattened))
    else:
        return res

