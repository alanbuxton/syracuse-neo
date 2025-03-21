import logging
from neomodel import db
from datetime import datetime, timezone, timedelta
from .models import Article, IndustryCluster
from .activity_helpers import activities_by_industry, activities_by_region, activities_by_source
from .industry_geo import COUNTRY_CODE_TO_NAME
from django.core.cache import cache

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
