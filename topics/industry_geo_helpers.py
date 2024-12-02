from topics.models import IndustryCluster, Organization
from topics.geo_utils import *
from collections import defaultdict
import logging 

logger = logging.getLogger(__name__)

NO_GEO_KEY="NOT SPECIFIED"

def orgs_by_industry_geo(industry_search_str):
    geo_to_country, geo_to_country_admin1 = get_geo_lookups()
    by_cluster = search_orgs_by_industry_clusters(industry_search_str, 
                                            geo_to_country, geo_to_country_admin1)
    by_industry_name = orgs_by_industry_text(industry_search_str, 
                                                             geo_to_country, geo_to_country_admin1)
    return by_cluster, by_industry_name

def prepare_industry_table(industry_search_str):
    by_cluster, by_industry_name = orgs_by_industry_geo(industry_search_str)
    industry_clusters = sorted_industry_clusters(by_cluster)
    region_codes = sorted_region_codes(by_cluster, by_industry_name)
    industry_table = dicts_to_table(industry_clusters, region_codes, by_cluster, by_industry_name, industry_search_str)
    return region_codes, industry_table

def dicts_to_table(industry_clusters, region_codes, by_cluster, by_industry_name, industry_search_str):
    results_table = []
    for industry_cluster in industry_clusters:
        results_row = [industry_cluster.topicId, industry_cluster.longest_representative_doc]
        for region_code in region_codes:
            val = by_cluster[industry_cluster.uri].get(region_code,[])
            results_row.append(len(val))
        results_table.append(results_row)
    results_row = ['',industry_search_str]
    for region_code in region_codes:
        val = by_industry_name.get(region_code,[])
        results_row.append(len(val))
    results_table.append(results_row)
    return results_table

def get_geo_lookups():
    _,_,_,geonameid_to_country_admin1 = get_geo_data()
    geo_to_country = geonameid_to_country_admin1['geo_to_country']
    geo_to_country_admin1 = geonameid_to_country_admin1['geo_to_country_admin1']
    return geo_to_country, geo_to_country_admin1

def get_orgs_by_leaf_industry_clusters(geo_to_country, geo_to_country_admin1):
    industry_clusters = IndustryCluster.leaf_nodes_only()
    return orgs_by_specified_industry_clusters(industry_clusters,
                                                 geo_to_country,
                                                 geo_to_country_admin1)

def search_orgs_by_industry_clusters(industry_search_str, geo_to_country, geo_to_country_admin1):
    industry_clusters = IndustryCluster.by_representative_doc_words(industry_search_str)
    return orgs_by_specified_industry_clusters(industry_clusters,
                                                 geo_to_country,
                                                 geo_to_country_admin1)

def orgs_by_specified_industry_clusters(industry_clusters,
                               geo_to_country,
                               geo_to_country_admin1):

    # counts_table = defaultdict(dict)
    orgs_table = defaultdict(dict)
    seen_uris = set()
    for ic in industry_clusters:
        if P.has_industry_cluster_geo_orgs(ic.topicId):
            # counts_table[ic.uri] = P.get_industry_cluster_geo_counts(ic.topicId)
            orgs_table[ic.uri] = P.get_industry_cluster_geo_orgs(ic.topicId)
        else:
            logger.info(f"Updating industry cluster geo orgs for {ic.uri}")
            # counts_table[ic.uri] = defaultdict(int)
            orgs_table[ic.uri] = defaultdict(set) # will become a list after persisting but for now will ensure we don't add duplicates
            for org in ic.mergedOrgsPrimary:
                if org.uri in seen_uris:
                    continue
                has_geoname = False
                for loc in org.basedInHighGeoNamesLocation:
                    country_code = geo_to_country.get(loc.geoNamesId)
                    if country_code is None:
                        logger.error(f"Couldn't find {loc} from org {org.uri} in geo_to_country dict")
                        continue
                    # counts_table[ic.uri][country_code] += 1
                    orgs_table[ic.uri][country_code].add( (org.uri, org.best_name) )
                    has_geoname = True
                    country_admin1 = geo_to_country_admin1.get(loc.geoNamesId)
                    if country_admin1 is not None:
                        ca_key = f"{country_admin1[0]}-{country_admin1[1]}"
                        # counts_table[ic.uri][ca_key] += 1
                        orgs_table[ic.uri][ca_key].add( (org.uri, org.best_name ))
                    logger.debug(f"{org.uri} - {loc.uri} - {country_code} - {country_admin1}")
                if has_geoname is False:
                    # counts_table[ic.uri][NO_GEO_KEY] += 1
                    orgs_table[ic.uri][NO_GEO_KEY].add( (org.uri, org.best_name) )
                seen_uris.add(org.uri)
            P.set_industry_cluster_geo_orgs(ic.topicId,orgs_table[ic.uri])

    return orgs_table 

def orgs_by_industry_text(industry_search_str,
                                            geo_to_country,
                                            geo_to_country_admin1):

    industry_search_str = industry_search_str.lower()

    if P.has_industry_cluster_geo_orgs(industry_search_str):
        orgs_table = P.get_industry_cluster_geo_orgs(industry_search_str)
    else:
        logger.info( f"Updating orgs by industry_search_str for {industry_search_str}" )
        matching_orgs = Organization.by_industry_text(industry_search_str) # only includes merged orgs
        # counts_table = defaultdict(int)
        orgs_table = defaultdict(list)

        for org in matching_orgs:
            has_geoname = False
            for loc in org.basedInHighGeoNamesLocation:
                country_code = geo_to_country.get(loc.geoNamesId)
                if country_code is None:
                    logger.error(f"Couldn't find {loc} from org {org.uri} in geo_to_country dict")
                    continue
                # counts_table[country_code] += 1
                orgs_table[country_code].append( (org.uri,org.best_name) )
                has_geoname = True
                country_admin1 = geo_to_country_admin1.get(loc.geoNamesId)
                if country_admin1 is not None:
                    ca_key = f"{country_admin1[0]}-{country_admin1[1]}"
                    # counts_table[ca_key] += 1
                    orgs_table[ca_key].append( (org.uri, org.best_name) )
            if has_geoname is False:
                # counts_table[NO_GEO_KEY] += 1
                orgs_table[NO_GEO_KEY].append( (org.uri, org.best_name) )

    return orgs_table

def sorted_industry_clusters(org_table):
    topic_uris = org_table.keys()
    topic_uris = list(topic_uris)
    industry_clusters = IndustryCluster.nodes.filter(uri__in=topic_uris)
    industry_clusters = sorted(industry_clusters, key=lambda x: x.longest_representative_doc)
    return industry_clusters

def sorted_region_codes(industry_cluster_org_table, industry_text_org_table):
    region_codes = set(industry_text_org_table.keys())
    for industry_uri, vals in industry_cluster_org_table.items():
        region_codes.update(vals.keys())
    try:
        region_codes.remove(NO_GEO_KEY)
        has_no_geo = True
    except KeyError:
        has_no_geo = False
    region_codes = sorted(region_codes)
    if has_no_geo is True:
        region_codes = region_codes + [NO_GEO_KEY]
    return region_codes

