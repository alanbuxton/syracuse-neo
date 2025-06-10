import logging
from neomodel import db
from datetime import datetime, timezone, timedelta
from topics.models import Article, IndustryCluster
from topics.activity_helpers import activities_by_source
from topics.industry_geo import COUNTRY_CODE_TO_NAME, org_uris_by_industry_id_country_admin1
from django.core.cache import cache
from topics.industry_geo.orgs_by_industry_geo import get_org_activity_counts, cached_activity_stats_last_updated_date
from topics.util import date_minus, min_and_max_date

logger = logging.getLogger(__name__)

def get_cached_stats(max_sources=100):
    latest_date = cached_activity_stats_last_updated_date()
    if latest_date is None:
        return None, None, None, None, None
    counts, recents_by_country_region, recents_by_source, recents_by_industry = get_stats(latest_date)
    recents_by_source = sorted(recents_by_source, key=lambda x: (-x[1],x[0]) )[:max_sources]
    return latest_date, counts, recents_by_country_region, recents_by_source, recents_by_industry

def industry_orgs_activities_stats(search_str):
    max_date = cached_activity_stats_last_updated_date()
    if max_date is None:
        return None
    logger.debug("industry_orgs_results started")
    ind_clusters = IndustryCluster.by_representative_doc_words(search_str)
    orgs_and_activity_counts_by_industry = {}
    days_ago_90 = date_minus(max_date,90)
    days_ago_30 = date_minus(max_date,30)
    days_ago_7 = date_minus(max_date,7)
    for ind in ind_clusters:
        orgs_and_activity_counts_by_industry[ind.topicId] = {}
        orgs_and_activity_counts_by_industry[ind.topicId]['industry'] = ind
        org_counts = len(org_uris_by_industry_id_country_admin1(ind.topicId,None,None))
        orgs_and_activity_counts_by_industry[ind.topicId]['orgs'] = org_counts
        cnt90 = activity_counts_by_industry(ind,days_ago_90,max_date)
        cnt30 = activity_counts_by_industry(ind,days_ago_30,max_date) if cnt90 > 0 else 0
        cnt7 = activity_counts_by_industry(ind,days_ago_7,max_date) if cnt30 > 0 else 0
        orgs_and_activity_counts_by_industry[ind.topicId]['activities_7'] = cnt7 
        orgs_and_activity_counts_by_industry[ind.topicId]['activities_30'] = cnt30
        orgs_and_activity_counts_by_industry[ind.topicId]['activities_90'] = cnt90
        
    dates =  {"max_date":max_date, "7": days_ago_7, "30": days_ago_30, "90": days_ago_90}
    return orgs_and_activity_counts_by_industry, dates


def activity_counts_by_industry(industry,min_date,max_date):
    if isinstance(industry, IndustryCluster):
        industry = industry.topicId
    results = get_org_activity_counts(min_date,max_date,industry,None,None)
    return results

def activity_counts_by_region(country_code,min_date,max_date):
    results = get_org_activity_counts(min_date,max_date,None,country_code,None)
    return results

def activity_counts_by_source(source_name,min_date,max_date):
    results = activities_by_source(source_name, min_date, max_date)
    return len(results)

def get_stats(max_date):
    _, max_date = min_and_max_date({"max_date":max_date})
    counts = []
    for x in ["Organization","Person","CorporateFinanceActivity","RoleActivity","LocationActivity","PartnershipActivity","ProductActivity",
              "Article","Role","AboutUs","AnalystRatingActivity","EquityActionsActivity","FinancialReportingActivity",
              "FinancialsActivity","IncidentActivity","MarketingActivity","OperationsActivity","RecognitionActivity","RegulatoryActivity"]:
        res, _ = db.cypher_query(f"""MATCH (n:{x}) WHERE
                    n.internalMergedSameAsHighToUri IS NULL
                    AND n.internalMergedActivityWithSimilarRelationshipsToUri IS NULL
                    RETURN COUNT(n)""")
        counts.append( (x , res[0][0]) )
    recents_by_country_region = []
    ts1 = datetime.now(tz=timezone.utc)
    recents_by_industry = []
    logger.info("Stats by industry")
    for industry in sorted(IndustryCluster.used_leaf_nodes_only(),
                           key=lambda x: x.longest_representative_doc):
        logger.debug(f"Stats for {industry.uri}")
        cnt90 = activity_counts_by_industry(industry.topicId,date_minus(max_date,90),max_date)
        cnt30 = activity_counts_by_industry(industry.topicId,date_minus(max_date,30),max_date) if cnt90 > 0 else 0
        cnt7 = activity_counts_by_industry(industry.topicId,date_minus(max_date,7),max_date) if cnt30 > 0 else 0
        if cnt90 > 0:
            recents_by_industry.append( 
                (industry.topicId, industry.longest_representative_doc,cnt7,cnt30,cnt90) )
    logger.info("Stats by country")
    for country_code,country_name in COUNTRY_CODE_TO_NAME.items():
        logger.debug(f"Stats for {country_code}")
        if country_code.strip() == '':
            continue
        cnt90 = activity_counts_by_region(country_code,date_minus(max_date,90),max_date)
        cnt30 = activity_counts_by_region(country_code,date_minus(max_date,30),max_date) if cnt90 > 0 else 0
        cnt7 = activity_counts_by_region(country_code,date_minus(max_date,7),max_date) if cnt30 > 0 else 0
        if cnt90 > 0:
            recents_by_country_region.append( (country_code,country_name,cnt7,cnt30,cnt90) )
    recents_by_source = []
    logger.info("Stats by source organization")
    for source_name in Article.all_sources():
        logger.debug(f"Stats for {source_name}")
        cnt90 = activity_counts_by_source(source_name, date_minus(max_date,90), max_date)
        cnt30 = activity_counts_by_source(source_name, date_minus(max_date,30), max_date) if cnt90 > 0 else 0
        cnt7 = activity_counts_by_source(source_name, date_minus(max_date,7), max_date) if cnt30 > 0 else 0
        if cnt90 > 0:
            recents_by_source.append( (source_name,cnt7,cnt30,cnt90) )
    ts2 = datetime.now(tz=timezone.utc)
    logger.info(f"counts_by_timedelta up to {max_date}: {ts2 - ts1}")
    return counts, recents_by_country_region, recents_by_source, recents_by_industry
