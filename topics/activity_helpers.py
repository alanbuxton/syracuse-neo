from django.core.cache import cache
from neomodel import db
import logging
from .models import IndustryCluster, Article, ActivityMixin, Resource, Organization
from .industry_geo.region_hierarchies import COUNTRY_CODE_TO_NAME
from .neo4j_utils import date_to_cypher_friendly, neo4j_date_converter
from .util import cache_friendly
from .industry_geo import geo_to_country_admin1

ORG_ACTIVITY_LIST="|".join([f"{x}Activity" for x in ["CorporateFinance","Product","Location","Partnership"]])

logger = logging.getLogger(__name__)

def get_activities_by_country_and_date_range(geo_code,min_date,max_date,limit=20):
    country, admin1_code = geo_to_country_admin1(geo_code)
    activity_article_uris = activities_by_region(country,min_date,max_date,
                                             admin1_code=admin1_code,counts_only=False,limit=limit)
    return activity_articles_to_api_results(activity_article_uris)


def get_activities_by_source_and_date_range(source_name, min_date, max_date, limit=20):
    activity_article_uris = activities_by_source(source_name, min_date, max_date, limit=limit)
    return activity_articles_to_api_results(activity_article_uris)


def get_activities_by_industry_and_date_range(industry, min_date, max_date, limit=20):
    activity_article_uris = activities_by_industry(industry, min_date=min_date,
                                max_date=max_date, limit=limit)
    return activity_articles_to_api_results(activity_article_uris)


def get_activities_by_org_uris_and_date_range(uri_list,min_date,max_date,combine_same_as_name_only=True,limit=None):
    uris_to_check = set(uri_list)
    if combine_same_as_name_only is True:
        orgs = Organization.nodes.filter(uri__in=uri_list)
        for org in orgs:
            new_uris = [x.uri for x in org.sameAsNameOnly]
            uris_to_check.update(new_uris)
    activity_article_uris = activities_by_org_uris(uris_to_check,min_date,max_date,limit)
    return activity_articles_to_api_results(activity_article_uris)


def get_activities_by_industry_geo_and_date_range(industry_id, geo_code, min_date, max_date,limit=None):
    country_code, admin1_code = geo_to_country_admin1(geo_code)
    industry = IndustryCluster.nodes.get_or_none(topicId=industry_id)
    activity_article_uris = activities_by_industry_region(industry,country_code,admin1_code,min_date,max_date)
    return activity_articles_to_api_results(activity_article_uris)


def activities_by_org_uris(org_uris, min_date, max_date, limit=None):
    cache_key = cache_friendly(f"activities_{org_uris}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        return res
    where_etc = f"""
        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')   
        AND o.uri in {list(org_uris)}
        RETURN DISTINCT act.uri, art.uri, art.datePublished
        ORDER BY art.datePublished DESC
    """
    limit_str = f" LIMIT {limit} " if limit else ""
    query = f"""
        MATCH (art: Article)<-[:documentSource]-(act:CorporateFinanceActivity|LocationActivity|PartnershipActivity)--(o: Resource&Organization)
        {where_etc}
        {limit_str}
        UNION
        MATCH (art: Article)<-[:documentSource]-(act:RoleActivity)--(p: Role)--(o: Resource&Organization)
        {where_etc}
        {limit_str}
    """
    return query_and_cache(query, cache_key, False)

def activities_by_source(source_name, min_date, max_date, counts_only=False,limit=None):
    cache_key = cache_friendly(f"activities_{source_name}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        return len(res) if counts_only is True else res
    limit_str = f" LIMIT {limit} " if limit else ""
    where_etc = f"""
        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')   
        AND art.sourceOrganization = '{source_name}'
        RETURN DISTINCT act.uri, art.uri, art.datePublished
        ORDER BY art.datePublished DESC
    """
    query = f"""MATCH (art: Article)--(act:{ORG_ACTIVITY_LIST})--(Organization)
    {where_etc}
    {limit_str}
    UNION
    MATCH (art: Article)--(act:RoleActivity)--(Role)--(Organization)
    {where_etc}
    {limit_str}
    """
    return query_and_cache(query, cache_key, counts_only)

def activities_by_industry_region(industry,country_code,admin1_code,min_date,max_date,limit=None):
    cache_key = cache_friendly(f"activities_{industry.topicId}_{country_code}_{admin1_code}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        return res
    admin1_str = f" AND l.admin1Code = '{admin1_code}' " if admin1_code else "" 
    limit_str = f" LIMIT {limit} " if limit else ""
    where_etc = f"""
        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')   
        AND l.countryCode = '{country_code}'
        {admin1_str}
        RETURN DISTINCT act.uri, art.uri, art.datePublished
        ORDER BY art.datePublished DESC
    """
    query = f"""MATCH (art: Article)--(act:{ORG_ACTIVITY_LIST})--(o:Organization)-[:basedInHighGeoNamesLocation]-(l:GeoNamesLocation)
    , (o)-[:industryClusterPrimary]-(ind:Resource&IndustryCluster {{uri: "{industry.uri}" }})
    {where_etc}
    {limit_str}
    UNION
    MATCH (art: Article)--(act:RoleActivity)--(r:Role)--(o:Organization)-[:basedInHighGeoNamesLocation]-(l:GeoNamesLocation)
    , (o)-[:industryClusterPrimary]-(ind:Resource&IndustryCluster {{uri: "{industry.uri}" }})
    {where_etc}
    {limit_str}
    """
    return query_and_cache(query, cache_key, counts_only=False)

    
def activities_by_region(country_code, min_date, max_date, admin1_code=None, counts_only=False,limit=None):
    cache_key=cache_friendly(f"activities_{country_code}_{admin1_code}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        return len(res) if counts_only is True else res
    admin1_str = f" AND l.admin1Code = '{admin1_code}' " if admin1_code else ""
    where_etc = f"""
        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')   
        AND l.countryCode = '{country_code}'
        {admin1_str}
        RETURN DISTINCT act.uri, art.uri, art.datePublished
        ORDER BY art.datePublished DESC
    """
    limit_str = f" LIMIT {limit} " if limit else ""
    query = f"""MATCH (art: Article)--(act:{ORG_ACTIVITY_LIST})--(Organization)-[:basedInHighGeoNamesLocation]-(l:GeoNamesLocation)
    {where_etc}
    {limit_str}
    UNION
    MATCH (art: Article)--(act:RoleActivity)--(Role)--(Organization)-[:basedInHighGeoNamesLocation]-(l:GeoNamesLocation)
    {where_etc}
    {limit_str}
    """
    return query_and_cache(query, cache_key, counts_only)


def activities_by_industry(industry, min_date, max_date, counts_only=False, limit=None):
    cache_key = cache_friendly(f"activities_{industry.topicId}_{min_date}_{max_date}_{limit}")
    res = cache.get(cache_key)
    if res is not None:
        return len(res) if counts_only is True else res
    where_etc = f"""
        WHERE art.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
        AND art.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
        RETURN DISTINCT act.uri, art.uri, art.datePublished
        ORDER BY art.datePublished DESC
    """
    limit_str = f" LIMIT {limit} " if limit else ""
    query = f"""MATCH (art: Article)--(act:{ORG_ACTIVITY_LIST})--(Organization)--(ind:Resource&IndustryCluster {{uri: "{industry.uri}" }})
    {where_etc}
    {limit_str}
    UNION
    MATCH (art: Article)--(act:RoleActivity)--(Role)--(Organization)--(ind:Resource&IndustryCluster {{uri: "{industry.uri}" }})
    {where_etc}
    {limit_str}
    """
    return query_and_cache(query, cache_key, counts_only)

def query_and_cache(query, cache_key, counts_only):
    vals, _ = db.cypher_query(query)
    vals = neo4j_date_converter(vals)
    cache.set(cache_key, vals)
    if counts_only is True:
        return len(vals)
    else:
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
        for actor_role, actor in activity.all_actors.items():
            if actor is not None and actor != []:
                if actors.get(actor_role) is None:
                    actors[actor_role] = set()
                actors[actor_role].update(actor)
        api_row["actors"] = actors
        api_results.append(api_row)
    return api_results