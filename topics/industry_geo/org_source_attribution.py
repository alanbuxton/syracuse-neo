'''
    Helpers to identify which source articles an organization got its industry or geonames locations
'''

import logging
from neomodel import db
from topics.models import IndustryCluster, GeoNamesLocation
from topics.util import geo_to_country_admin1
logger = logging.getLogger(__name__)

OTHER_INDUSTRY = 'industry'
OTHER_GEO_STRING = 'geo'

def get_source_orgs_and_weights(source_org, other_type, other_entity):
    if other_type == OTHER_INDUSTRY:
        query = f"""MATCH (n: Resource&IndustryCluster {{uri:'{other_entity.uri}'}})-
                [r:industryClusterPrimary]-
                (o: Resource&Organization {{internalMergedSameAsHighToUri:'{source_org.uri}'}})
                RETURN o, r.weight
                """
    elif other_type == OTHER_GEO_STRING:
        country, admin1 = other_entity
        admin1_str = f" AND n.admin1Code = '{admin1}' " if admin1 is not None else ""
        query = f"""MATCH (n: Resource&GeoNamesLocation)-
                [r: basedInHighGeoNamesLocation]-
                (o: Resource&Organization {{internalMergedSameAsHighToUri:'{source_org.uri}'}})
                WHERE n.countryCode = '{country}' 
                {admin1_str}
                RETURN o, sum(r.weight)
                """
    logger.debug(query)
    orgs_and_weights, _ = db.cypher_query(query,resolve_objects=True)
    return orgs_and_weights

def get_my_industry_weight(org,industry):
    query = f"""MATCH (n: Resource {{uri:'{org.uri}'}})-
                [rel:industryClusterPrimary]-
                (ind: Resource {{uri:'{industry.uri}'}}) RETURN rel.weight"""
    weights, _ = db.cypher_query(query)
    if len(weights) == 0:
        return None
    return weights[0][0]

def get_my_geo_weight(org, country, admin1):
    admin1_str = f" AND geo.admin1Code = '{admin1}' " if admin1 is not None else ""
    query = f"""MATCH (n: Resource {{uri:'{org.uri}'}})-
            [rel:basedInHighGeoNamesLocation]-
            (geo: Resource&GeoNamesLocation) 
            WHERE geo.countryCode = '{country}'
            {admin1_str}
            RETURN rel.weight"""
    logger.debug(query)
    weights, _ = db.cypher_query(query)
    if len(weights) == 0:
        return None
    return sum([x[0] for x in weights])


def get_source_orgs_for_ind_cluster_or_geo_code(self, other_entity, my_weight = None, leaf_orgs=None):
    if isinstance(other_entity, IndustryCluster):
        other_type = OTHER_INDUSTRY
    elif isinstance(other_entity, str):
        other_type = OTHER_GEO_STRING
        country, admin1 = geo_to_country_admin1(other_entity)
    else:
        raise ValueError(f"Unexpected object {other_entity}")

    if leaf_orgs is None:
        leaf_orgs = set()

    if my_weight is None: # Start off finding my weight
        if other_type == OTHER_INDUSTRY:
            my_weight = get_my_industry_weight(self, other_entity)
        elif other_type == OTHER_GEO_STRING:
            my_weight = get_my_geo_weight(self, country, admin1)
        if my_weight is None:
            logger.warning(f"{self} is not connected to {other_entity}")
            return leaf_orgs
        logger.debug(f"Target Org {self.uri} weight = {my_weight}")
            
    logger.debug(f"Working on items merged into {self.uri}, leaf_orgs = {leaf_orgs}")

    source_weights = 0
    if other_type == OTHER_INDUSTRY:
        ind_or_country_admin1 = other_entity
    elif other_type == OTHER_GEO_STRING:
        ind_or_country_admin1 = (country, admin1)
    source_orgs_and_weights = get_source_orgs_and_weights(self, other_type, ind_or_country_admin1)
    for source_org, weight in source_orgs_and_weights:
        logger.debug(f"Source Org {source_org.uri} weight = {weight}")
        source_weights += weight
        if weight == 1 and len(get_source_orgs_and_weights(source_org, other_type, ind_or_country_admin1)) == 0:   
            leaf_orgs.add((source_org, weight))           
            logger.debug(f"Adding leaf node {source_org.uri} - currently have {len(leaf_orgs)} leaves")
        else:
            leaf_orgs = get_source_orgs_for_ind_cluster_or_geo_code(source_org, other_entity, weight, leaf_orgs)
    if source_weights < my_weight:
        logger.debug(f"Source weights add up to {source_weights}, my weight = {my_weight} - so adding {self.uri} as another source")
        leaf_orgs.add((self,my_weight))
    return leaf_orgs

def get_source_orgs_articles_for(self, other_object,limit=10):
    leaf_orgs = get_source_orgs_for_ind_cluster_or_geo_code(self, other_object)
    orgs_and_articles = [(x[0],x[1],x[0].documentSource.filter(internalDocId=x[0].internalDocId)[0]) for x in leaf_orgs]
    orgs_and_articles = sorted(orgs_and_articles, key=lambda x: x[2].datePublished,reverse=True)[:limit]
    return orgs_and_articles