from django.core.cache import cache
from neomodel import db
import logging
from topics.models import IndustryCluster, Article, ActivityMixin, Resource
from .industry_geo.region_hierarchies import COUNTRY_CODE_TO_NAME
from topics.industry_geo.orgs_by_industry_geo import get_org_activities
from .neo4j_utils import date_to_cypher_friendly, neo4j_date_converter, clean_str
from .util import cache_friendly
from .industry_geo import geo_to_country_admin1
from .organization_search_helpers import get_same_as_name_onlies
from topics.util import ALL_ACTIVITY_LIST, ORG_ACTIVITY_LIST

logger = logging.getLogger(__name__)

def get_activities_by_country_and_date_range(geo_code,min_date,max_date,limit=20):
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    activity_article_uris = get_org_activities(min_date,max_date,None,country_code,admin1_code)
    return activity_articles_to_api_results(activity_article_uris,limit)

def get_activities_by_source_and_date_range(source_name, min_date, max_date, limit=20):
    activity_article_uris = activities_by_source(source_name, min_date, max_date, limit)
    return activity_articles_to_api_results(activity_article_uris,limit)

def get_activities_by_industry_and_date_range(industry, min_date, max_date, limit=20):
    if isinstance(industry, IndustryCluster):
        industry = industry.topicId
    activity_article_uris = get_org_activities(min_date,max_date,industry,None,None)
    return activity_articles_to_api_results(activity_article_uris,limit)

def get_activities_by_industry_geo_and_date_range(industry, geo_code, min_date, max_date,limit=None):
    if isinstance(industry, IndustryCluster):
        industry = industry.topicId
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    activity_article_uris = get_org_activities(min_date,max_date,industry,country_code, admin1_code)
    return activity_articles_to_api_results(activity_article_uris,limit)

def get_activities_by_industry_country_admin1_and_date_range(industry, country_code, admin1_code, min_date, max_date,limit=None):
    if isinstance(industry, IndustryCluster):
        industry = industry.topicId
    activity_article_uris = get_org_activities(min_date,max_date,industry,country_code, admin1_code)
    return activity_articles_to_api_results(activity_article_uris,limit)

def activities_by_industry(industry, min_date, max_date, limit=None):
    if isinstance(industry, IndustryCluster):
        industry = industry.topicId
    activity_article_uris = get_org_activities(min_date,max_date,industry,None, None)
    return activity_article_uris[:limit]

def get_activities_by_org_and_date_range(organization,min_date,max_date,include_similar_orgs=False,combine_same_as_name_only=True,limit=None):
    uri_list = [organization.uri]
    return get_activities_by_org_uris_and_date_range(uri_list,min_date,max_date,combine_same_as_name_only,limit)

def get_activities_by_org_uris_and_date_range(uri_list,min_date,max_date,combine_same_as_name_only=True,limit=None):
    logger.debug(f"get_activities_by_org_uris_and_date_range: org uris {len(uri_list)} {min_date} {max_date}, combine_same_as_name_only {combine_same_as_name_only} limit {limit}")
    activity_article_uris = activities_by_org_uris_incl_same_as(uri_list,min_date,max_date,combine_same_as_name_only,limit)
    res = activity_articles_to_api_results(activity_article_uris)
    logger.debug(f"activity articles prepared")
    return res

def activities_by_org_uris_incl_same_as(uri_list,min_date,max_date,combine_same_as_name_only=True,limit=None):
    uris_to_check = set(uri_list)
    if combine_same_as_name_only is True:
        orgs = Resource.nodes.filter(uri__in=uri_list)
        for org in orgs:
            new_uris = [x.uri for x in get_same_as_name_onlies(org)]
            uris_to_check.update(new_uris)
    logger.debug(f"same-as-name-only updated. going to kick off query")
    activity_article_uris = activities_by_org_uris(uris_to_check,min_date,max_date,limit)
    logger.debug(f"query done")
    return activity_article_uris

def activities_by_org_uris(org_uris, min_date, max_date, limit=None):
    logger.debug(f"activities_by_org_uris {len(org_uris)} org_uris, {min_date} - {max_date}")
    org_uris = sorted(org_uris)
    cache_key = cache_friendly(f"activities_{org_uris}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        logger.debug(f"activities_by_org_uris {cache_key} cache hit")
        return res
    logger.debug(f"activities_by_org_uris {cache_key} cache miss")
    where_etc = f"""
        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')   
        AND act.internalMergedActivityWithSimilarRelationshipsToUri IS NULL
        AND o.uri in {list(org_uris)}
        RETURN DISTINCT act.uri, art.uri, art.datePublished
        ORDER BY art.datePublished DESC
    """
    limit_str = f" LIMIT {limit} " if limit else ""
    query = f"""
        MATCH (art: Article)<-[:documentSource]-(act:{ORG_ACTIVITY_LIST})--(o: Resource&Organization)
        {where_etc}
        {limit_str}
        UNION
        MATCH (art: Article)<-[:documentSource]-(act:RoleActivity)-[:withRole]->(Role)<-[:hasRole]-(o: Resource&Organization)
        {where_etc}
        {limit_str}
    """
    return query_and_cache(query, cache_key)

def activities_by_source(source_name, min_date, max_date, limit=None):
    cache_key = cache_friendly(f"activities_{source_name}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        return res
    limit_str = f" LIMIT {limit} " if limit is not None else ""
    query = f"""
    MATCH (art: Article)<-[:documentSource]-(act:{ALL_ACTIVITY_LIST})
    WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
    AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}') 
    AND art.sourceOrganization = '{clean_str(source_name)}'
    AND act.internalMergedActivityWithSimilarRelationshipsToUri IS NULL
    AND EXISTS {{
        MATCH (art)<-[:documentSource]-(org: Organization)
        WHERE org.internalMergedSameAsHighToUri IS NULL
    }}
    RETURN DISTINCT act.uri, art.uri, art.datePublished
    ORDER by art.datePublished DESC
    {limit_str}
    """
    return query_and_cache(query, cache_key)


def query_and_cache(query, cache_key):
    logger.debug(query)
    vals, _ = db.cypher_query(query)
    vals = neo4j_date_converter(vals)
    cache.set(cache_key, vals)
    return vals


def activity_articles_to_api_results(activity_article_uris, limit=None):
    api_results = []
    by_date = sorted(activity_article_uris,key=lambda x: x[2], reverse=True)
    if limit is not None:
        by_date = by_date[:limit]
    for activity_uri, article_uri, date_published in activity_article_uris:
        activity = Resource.nodes.get_or_none(uri=activity_uri)
        article = Resource.nodes.get_or_none(uri=article_uri)
        assert isinstance(article, Article), f"{article} should be an Article"
        assert isinstance(activity, ActivityMixin), f"{activity} should be an Activity"
        api_row = {}
        api_row["source_organization"] = article.sourceOrganization
        api_row["date_published"] = date_published
        api_row["headline"] = article.headline
        api_row["document_extract"] = activity.documentSource.relationship(article).documentExtract
        api_row["document_url"] = article.documentURL
        api_row["archive_org_page_url"] = article.archiveOrgPageURL
        api_row["archive_org_list_url"] = article.archiveOrgListURL
        api_row["activity_uri"] = activity.uri
        api_row["activity_locations"] = activity.whereHighGeoNamesLocation
        api_row["activity_location_as_string"] = activity.whereHighGeoName_as_str
        api_row["activity_class"] = activity.__class__.__name__
        api_row["activity_types"] = activity.activityType
        api_row["activity_longest_type"] = activity.longest_activityType
        api_row["activity_statuses"] = activity.status
        api_row["activity_status_as_string"] = activity.status_as_string
        api_row["source_is_core"] = article.is_core
        actors = {}
        for actor_role, actor_list in activity.all_actors.items():
            if actor_list is not None and actor_list != []:
                if actors.get(actor_role) is None:
                    actors[actor_role] = set()
                actors[actor_role].update(actor_list)
        api_row["actors"] = actors
        api_results.append(api_row)
    return api_results