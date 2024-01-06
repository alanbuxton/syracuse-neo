from neomodel import db
from .models import Organization, ActivityMixin
from datetime import datetime, timezone, timedelta
from typing import List, Union
import logging
from django.core.cache import cache

logger = logging.getLogger(__name__)

def get_relevant_orgs_for_country(country_code):
    from .geo_utils import COUNTRY_CODES
    if country_code is None or country_code not in COUNTRY_CODES.keys():
        logger.debug(f"{country_code} is not a known country_code")
        return set()
    ts1 = datetime.utcnow()
    orgs = Organization.based_in_country(country_code)
    ts2 = datetime.utcnow()
    orgs_by_activity = ActivityMixin.orgs_by_activity_where(country_code)
    ts3 = datetime.utcnow()
    logger.info(f"{country_code} orgs took: {ts2 - ts1}; orgs by act took: {ts3 - ts2}")
    all_orgs = set(orgs + orgs_by_activity)
    return all_orgs

def get_activities_by_date_range_for_api(min_date, uri_or_list: Union[str,List[str]],
                                            max_date = datetime.now(tz=timezone.utc),
                                            limit = None, include_same_as=True):
    assert min_date is not None, "Must have min date"
    if uri_or_list is None or len(uri_or_list) == 0 or set(uri_or_list) == {None}:
        return []
    objs = get_activities_by_date_range(min_date, max_date, uri_or_list, limit,include_same_as)
    api_results = []
    for row in objs:
        activity = row[0]
        org = row[1]
        assert isinstance(activity, ActivityMixin)
        assert isinstance(org, Organization)
        api_row = {}
        api_row["source_name"] = activity.sourceName
        api_row["document_date"] = activity.documentDate
        api_row["document_title"] = activity.documentTitle
        api_row["document_extract"] = activity.documentExtract
        api_row["document_url"] = activity.documentURL[0].uri
        api_row["organization_names"] = org.name
        api_row["organization_longest_name"] = org.longest_name
        api_row["organization_uri"] = org.uri
        api_row["activity_uri"] = activity.uri
        api_row["activity_class"] = activity.__class__.__name__
        api_row["activity_types"] = activity.activityType
        api_row["activity_longest_type"] = activity.longest_activityType
        api_row["activity_statuses"] = activity.status
        api_row["activity_status_as_string"] = activity.status_as_string
        api_results.append(api_row)
    return api_results

def get_activities_by_date_range(min_date, max_date, uri_or_uri_list: Union[str,List], limit=None, include_same_as=True,
                                    counts_only = False):
    if isinstance(uri_or_uri_list, str):
        uri_list = [uri_or_uri_list]
    elif isinstance(uri_or_uri_list, set):
        uri_list = list(uri_or_uri_list)
    else:
        uri_list = uri_or_uri_list
    orgs = Organization.nodes.filter(uri__in=uri_list)
    uris_to_check = set(uri_list)
    if include_same_as is True:
        for org in orgs:
            new_uris = [x.uri for x in org.same_as()]
            uris_to_check.update(new_uris)
    if limit is not None:
        limit_str = f"LIMIT {limit}"
    else:
        limit_str = ""
    if counts_only is True:
        return_str = "RETURN COUNT(*)"
    else:
        return_str = "RETURN * ORDER BY n.documentDate DESC"
    query = f"""
        MATCH (n:CorporateFinanceActivity|RoleActivity|LocationActivity)--(o: Organization)
        WHERE n.documentDate >= datetime('{date_to_cypher_friendly(min_date)}')
        AND n.documentDate <= datetime('{date_to_cypher_friendly(max_date)}')
        AND o.uri IN {list(uris_to_check)}
        {return_str}
        {limit_str}
    """
    logger.debug(query)
    objs, _ = db.cypher_query(query, resolve_objects=True)
    return objs # Each row is tuple of activity, organization

def date_to_cypher_friendly(date):
    if isinstance(date, str):
        return datetime.fromisoformat(date).isoformat()
    else:
        return date.isoformat()

def get_stats(max_date):
    cache_key = f"stats_{max_date}"
    res = cache.get(cache_key)
    if res is not None:
        return res
    from .geo_utils import COUNTRY_NAMES
    counts = []
    for x in ["Organization","Person","CorporateFinanceActivity","RoleActivity","LocationActivity"]:
        res, _ = db.cypher_query(f"MATCH (n:{x}) RETURN COUNT(n)")
        counts.append( (x , res[0][0]) )
    recents = []
    ts1 = datetime.utcnow()
    for k,v in sorted(COUNTRY_NAMES.items()):
        cnt7 = counts_by_timedelta(7,max_date,v)
        cnt30 = counts_by_timedelta(30,max_date,v)
        cnt90 = counts_by_timedelta(90,max_date,v)
        if cnt7 > 0 or cnt30 > 0 or cnt90 > 0:
            recents.append( (v,k,cnt7,cnt30,cnt90) )
    ts2 = datetime.utcnow()
    logger.debug(f"counts_by_timedelta up to {max_date}: {ts2 - ts1}")
    cache.set( cache_key, (counts, recents) , timeout=60*60*48)
    return counts, recents

def counts_by_timedelta(days_ago, max_date, country_code):
    res = get_counts(country_code,max_date - timedelta(days=days_ago),max_date)
    return res

def get_counts(country_code,min_date,max_date):
    relevant_uris = get_relevant_orgs_for_country(country_code)
    uris = [x.uri for x in relevant_uris]
    counts = get_activities_by_date_range(min_date,max_date,uris,include_same_as=False,counts_only=True)
    return counts[0][0]
