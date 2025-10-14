import logging
from neomodel import db 
from topics.util import ALL_ACTIVITY_LIST
from syracuse.settings import GEO_LOCATION_MIN_WEIGHT_PROPORTION, INDUSTRY_CLUSTER_MIN_WEIGHT_PROPORTION
from topics.neo4j_utils import date_to_cypher_friendly
from topics.industry_geo.geoname_mappings import COUNTRIES_WITH_STATE_PROVINCE

logger = logging.getLogger(__name__)

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
    withs = ["o", "articles1 + articles2 as allArticleData"]
    unwinds = []
    if include_industry:
        to_return.append("ic.topicId")
        withs.append("ics")
        unwinds.append("unwind ics as ic")
    if include_geo:
        to_return.append("loc.countryCode")
        withs.append("locs")
        unwinds.append("unwind locs as loc")
    if include_admin1:
        to_return.append("loc.admin1Code")
    assert len(to_return) > 0, f"{to_return} has to have at least one item to return"
    with_stmt = "with " + ",".join(withs)
    unwind_stmt = " ".join(unwinds)
    return_stmt = f"RETURN {', '.join(to_return)} , collect([o.uri, apoc.node.degree(o), o.internalDocId, allArticleData])"
    return f"{with_stmt} {unwind_stmt} {return_stmt}"


def build_industry_section(return_as_collect=True):
    if return_as_collect:   
        return_section = "RETURN COLLECT(ic) AS ics" 
    else: 
        return_section = "RETURN ic ORDER BY weight DESCENDING"
    return f"""CALL {{
        WITH o
        MATCH (o)-[r:industryClusterPrimary]->(ic:IndustryCluster)
        WITH o, ic, r.weight AS weight
        MATCH (o)-[rAll:industryClusterPrimary]->(:IndustryCluster)
        WITH o, ic, weight, SUM(rAll.weight) AS totalWeight
        WHERE weight >= {INDUSTRY_CLUSTER_MIN_WEIGHT_PROPORTION} * totalWeight
        AND weight > 1
        {return_section}
        }}
    """

def build_geo_section(include_admin1=False, return_as_collect=True):
    admin1_section = build_admin1_section() if include_admin1 else ""
    if return_as_collect:   
        return_section = "RETURN COLLECT(loc) AS locs" 
    else: 
        return_section = "RETURN loc ORDER BY weight DESCENDING"
    return f"""CALL {{
        WITH o
        MATCH (o)-[r:basedInHighGeoNamesLocation]->(loc:GeoNamesLocation)
        {admin1_section}
        WITH o, loc, r.weight AS weight
        MATCH (o)-[rAll:basedInHighGeoNamesLocation]->(:GeoNamesLocation)
        WITH o, loc, weight, SUM(rAll.weight) AS totalWeight
        WHERE weight >= {GEO_LOCATION_MIN_WEIGHT_PROPORTION} * totalWeight
        AND weight > 1
        {return_section}
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
    RETURN collect([act.uri, art.uri, art.datePublished]) AS articles1

    }}
    CALL {{
    WITH o
    OPTIONAL MATCH (act2: RoleActivity)--(:Role)--(o)-[:documentSource]->(art2: Article)<-[:documentSource]-(act2)
    WHERE art2.datePublished >= datetime('{date_to_cypher_friendly(min_date)}')
    AND art2.datePublished <= datetime('{date_to_cypher_friendly(max_date)}')
    AND act2.internalMergedActivityWithSimilarRelationshipsToUri IS NULL
    RETURN collect([act2.uri, art2.uri, art2.datePublished]) AS articles2
   
    }}
    """

def industries_for_org(org_uri):
    query = f"""
    MATCH (o: Resource&Organization)
    WHERE o.uri = '{org_uri}'
    {build_industry_section(return_as_collect=False)}
    RETURN ic
    """
    vals, _ = db.cypher_query(query, resolve_objects=True)
    return [v[0] for v in vals]

def based_in_high_geo_names_locations_for_org(org_uri):
    query = f"""
    MATCH (o: Resource&Organization)
    WHERE o.uri = '{org_uri}'
    {build_geo_section(return_as_collect=False)}
    RETURN loc
    """
    vals, _ = db.cypher_query(query, resolve_objects=True)
    return [v[0] for v in vals]
