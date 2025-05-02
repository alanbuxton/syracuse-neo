import logging
from neomodel import db
from datetime import datetime, timezone, timedelta
from .models import Article, IndustryCluster
from .activity_helpers import activities_by_industry, activities_by_region, activities_by_source
from .industry_geo import COUNTRY_CODE_TO_NAME
from django.core.cache import cache
from topics.industry_geo.orgs_by_industry_geo import org_uris_by_industry_cluster_and_geo

logger = logging.getLogger(__name__)


def cached_activity_stats_last_updated_date():
    return cache.get("activity_stats_last_updated")

def date_minus(to_date, days):
    return to_date - timedelta(days=days)


def get_cached_stats():
    latest_date = cached_activity_stats_last_updated_date()
    if latest_date is None:
        return None, None, None, None, None
    counts, recents_by_country_region, recents_by_source, recents_by_industry = get_stats(latest_date)
    return latest_date, counts, recents_by_country_region, recents_by_source, recents_by_industry

def industry_orgs_activities_stats(search_str, max_date=None, include_search_by_industry_text=False, counts_only=True):
    assert include_search_by_industry_text is False, f"include_search_by_industry_text is not supported"
    logger.debug("industry_orgs_results started")
    ind_clusters = IndustryCluster.by_representative_doc_words(search_str)
    orgs_and_activities_by_industry = {}
    if max_date is None:
        max_date = cached_activity_stats_last_updated_date()
    if max_date is None:
        return None
    days_ago_90 = date_minus(max_date,90)
    days_ago_30 = date_minus(max_date,30)
    days_ago_7 = date_minus(max_date,7)
    for ind in ind_clusters:
        orgs_and_activities_by_industry[ind.topicId] = {}
        orgs_and_activities_by_industry[ind.topicId]['industry'] = ind
        org_stats = org_uris_by_industry_cluster_and_geo(ind.uri,ind.topicId,None)
        if counts_only is True:
            org_stats = len(org_stats)
        orgs_and_activities_by_industry[ind.topicId]['orgs'] = org_stats
        cnt90 = activities_by_industry(ind,days_ago_90,max_date,counts_only=True)
        cnt30 = activities_by_industry(ind,days_ago_30,max_date,counts_only=True) if cnt90 > 0 else 0
        cnt7 = activities_by_industry(ind,days_ago_7,max_date,counts_only=True) if cnt30 > 0 else 0

        orgs_and_activities_by_industry[ind.topicId]['activities_7'] = cnt7 
        orgs_and_activities_by_industry[ind.topicId]['activities_30'] = cnt30
        orgs_and_activities_by_industry[ind.topicId]['activities_90'] = cnt90
        
    dates =  {"max_date":max_date, "7": days_ago_7, "30": days_ago_30, "90": days_ago_90}
    return orgs_and_activities_by_industry, dates


def get_stats(max_date):
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
    for industry in sorted(IndustryCluster.leaf_nodes_only(),
                           key=lambda x: x.longest_representative_doc):
        logger.info(f"Stats for {industry.uri}")
        cnt90 = activities_by_industry(industry,date_minus(max_date,90),max_date,counts_only=True)
        cnt30 = activities_by_industry(industry,date_minus(max_date,30),max_date,counts_only=True) if cnt90 > 0 else 0
        cnt7 = activities_by_industry(industry,date_minus(max_date,7),max_date,counts_only=True) if cnt30 > 0 else 0
        if cnt90 > 0:
            recents_by_industry.append( 
                (industry.topicId, industry.longest_representative_doc,cnt7,cnt30,cnt90) )
    for country_code,country_name in COUNTRY_CODE_TO_NAME.items():
        logger.info(f"Stats for {country_code}")
        if country_code.strip() == '':
            continue
        cnt90 = activities_by_region(country_code,date_minus(max_date,90),max_date,counts_only=True)
        cnt30 = activities_by_region(country_code,date_minus(max_date,30),max_date,counts_only=True) if cnt90 > 0 else 0
        cnt7 = activities_by_region(country_code,date_minus(max_date,7),max_date,counts_only=True) if cnt30 > 0 else 0
        if cnt90 > 0:
            recents_by_country_region.append( (country_code,country_name,cnt7,cnt30,cnt90) )
    recents_by_source = []
    for source_name in Article.all_sources():
        logger.info(f"Stats for {source_name}")
        cnt90 = activities_by_source(source_name, date_minus(max_date,90), max_date,counts_only=True)
        cnt30 = activities_by_source(source_name, date_minus(max_date,30), max_date,counts_only=True) if cnt90 > 0 else 0
        cnt7 = activities_by_source(source_name, date_minus(max_date,7), max_date,counts_only=True) if cnt30 > 0 else 0
        if cnt90 > 0:
            recents_by_source.append( (source_name,cnt7,cnt30,cnt90) )
    ts2 = datetime.now(tz=timezone.utc)
    logger.info(f"counts_by_timedelta up to {max_date}: {ts2 - ts1}")
    return counts, recents_by_country_region, recents_by_source, recents_by_industry
