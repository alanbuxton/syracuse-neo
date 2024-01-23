from neomodel import db
from .models import Organization, ActivityMixin
from datetime import datetime, timezone, timedelta
from typing import List, Union
import logging
from django.core.cache import cache
from topics.geo_utils import geo_select_list

logger = logging.getLogger(__name__)

def get_activities_for_serializer_by_country_and_date_range(geo_code,min_date,max_date,limit=20,include_same_as=True):
    relevant_orgs = get_relevant_orgs_for_country_region(geo_code)
    relevant_uris = [x.uri for x in relevant_orgs]
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, uri_or_list=relevant_uris,
                                max_date=max_date, limit=limit, include_same_as=include_same_as)
    return matching_activity_orgs

def get_activities_for_serializer_by_source_and_date_range(source_name, min_date, max_date, limit=20):
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, source_name=source_name,
                                max_date=max_date, limit=limit, include_same_as=False)
    return matching_activity_orgs


def get_relevant_orgs_for_country_region(geo_code):
    if geo_code is None or geo_code.strip() == "":
        logger.debug(f"Can't process blank geo_code")
        return set()
    ts1 = datetime.utcnow()
    orgs = Organization.based_in_country_region(geo_code)
    ts2 = datetime.utcnow()
    orgs_by_activity = ActivityMixin.orgs_by_activity_where(geo_code)
    ts3 = datetime.utcnow()
    logger.info(f"{geo_code} orgs took: {ts2 - ts1}; orgs by act took: {ts3 - ts2}")
    all_orgs = set(orgs + orgs_by_activity)
    return all_orgs

def get_activities_by_date_range_for_api(min_date, uri_or_list: Union[str,List[str],None] = None,
                                            source_name: Union[str,None] = None,
                                            max_date = datetime.now(tz=timezone.utc),
                                            limit = None, include_same_as=True):
    assert min_date is not None, "Must have min date"
    if (uri_or_list is None or len(uri_or_list) == 0 or set(uri_or_list) == {None}) and source_name is None:
        return []
    if source_name is None:
        activities = get_activities_by_org_uri_and_date_range(uri_or_list, min_date, max_date, limit,include_same_as)
    else:
        activities = get_activities_by_source_and_date_range(source_name, min_date, max_date, limit, include_same_as)
    api_results = []
    for activity in activities:
        assert isinstance(activity, ActivityMixin), f"{activity} should be an Activity"
        api_row = {}
        api_row["source_name"] = activity.sourceName
        api_row["document_date"] = activity.documentDate
        api_row["document_title"] = activity.documentTitle
        api_row["document_extract"] = activity.documentExtract
        api_row["document_url"] = activity.documentURL[0].uri
        api_row["activity_uri"] = activity.uri
        api_row["activity_class"] = activity.__class__.__name__
        api_row["activity_types"] = activity.activityType
        api_row["activity_longest_type"] = activity.longest_activityType
        api_row["activity_statuses"] = activity.status
        api_row["activity_status_as_string"] = activity.status_as_string
        participants = {}
        for participant_role, participant in activity.all_participants.items():
            if participant is not None and participant != []:
                if participants.get(participant_role) is None:
                    participants[participant_role] = set()
                participants[participant_role].update(participant)
        api_row["participants"] = participants
        api_results.append(api_row)
    return api_results

def get_all_source_names():
    sources, _ = db.cypher_query("MATCH (n:CorporateFinanceActivity|RoleActivity|LocationActivity) RETURN DISTINCT n.sourceName;")
    flattened = [x for sublist in sources for x in sublist]
    return flattened

def get_activities_by_source_and_date_range(source_name,min_date, max_date, limit=None,counts_only=False):
    if counts_only is True:
        return_str = "RETURN COUNT(n)"
    else:
        return_str = "RETURN n ORDER BY n.documentDate DESC"
    if limit is not None:
        limit_str = f"LIMIT {limit}"
    else:
        limit_str = ""
    query = f"""MATCH (n:CorporateFinanceActivity|RoleActivity|LocationActivity)
                WHERE n.documentDate >= datetime('{date_to_cypher_friendly(min_date)}')
                AND n.documentDate <= datetime('{date_to_cypher_friendly(max_date)}')
                AND n.sourceName = ('{source_name}')
                {return_str} {limit_str};"""
    objs, _ = db.cypher_query(query, resolve_objects=True)
    flattened = [x for sublist in objs for x in sublist]
    return flattened

def get_activities_by_org_uri_and_date_range(uri_or_uri_list: Union[str,List], min_date, max_date, limit=None, include_same_as=True,
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
        return_str = "RETURN COUNT(DISTINCT(n))"
    else:
        return_str = "RETURN DISTINCT(n) ORDER BY n.documentDate DESC"
    query = f"""
        MATCH (n:CorporateFinanceActivity|RoleActivity|LocationActivity)--(o: Organization)
        WHERE n.documentDate >= datetime('{date_to_cypher_friendly(min_date)}')
        AND n.documentDate <= datetime('{date_to_cypher_friendly(max_date)}')
        AND o.uri IN {list(uris_to_check)}
        {return_str} {limit_str};
    """
    logger.debug(query)
    objs, _ = db.cypher_query(query, resolve_objects=True)
    flattened = [x for sublist in objs for x in sublist]
    return flattened

def date_to_cypher_friendly(date):
    if isinstance(date, str):
        return datetime.fromisoformat(date).isoformat()
    else:
        return date.isoformat()

def get_stats(max_date,allowed_to_set_cache=False):
    cache_key = f"stats_{max_date}"
    res = cache.get(cache_key)
    if res is not None:
        return res
    counts = []
    for x in ["Organization","Person","CorporateFinanceActivity","RoleActivity","LocationActivity"]:
        res, _ = db.cypher_query(f"MATCH (n:{x}) RETURN COUNT(n)")
        counts.append( (x , res[0][0]) )
    recents_by_country_region = []
    ts1 = datetime.utcnow()
    for k,v in geo_select_list():
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
        cnt7 = counts_by_timedelta(7,max_date,source_name=source_name)
        cnt30 = counts_by_timedelta(30,max_date,source_name=source_name)
        cnt90 = counts_by_timedelta(90,max_date,source_name=source_name)
        if cnt7 > 0 or cnt30 > 0 or cnt90 > 0:
            recents_by_source.append( (source_name,cnt7,cnt30,cnt90) )
    ts2 = datetime.utcnow()
    logger.debug(f"counts_by_timedelta up to {max_date}: {ts2 - ts1}")
    if allowed_to_set_cache is True:
        cache.set( cache_key, (counts, recents_by_country_region, recents_by_source) )
    else:
        logger.debug("Not allowed to set cache")
    return counts, recents_by_country_region, recents_by_source

def counts_by_timedelta(days_ago, max_date, geo_code=None,source_name=None):
    min_date = max_date - timedelta(days=days_ago)
    if geo_code is not None:
        res = get_country_region_counts(geo_code,min_date,max_date)
    elif source_name is not None:
        res = get_source_counts(source_name,min_date,max_date)
    else:
        raise ValueError(f"counts_by_timedelta must supplier geo_code or source_name")
    return res

def get_source_counts(source_name, min_date,max_date):
    counts = get_activities_by_source_and_date_range(source_name,min_date,max_date,counts_only=True)
    return counts[0]

def get_country_region_counts(geo_code,min_date,max_date):
    relevant_uris = get_relevant_orgs_for_country_region(geo_code)
    uris = [x.uri for x in relevant_uris]
    counts = get_activities_by_org_uri_and_date_range(uris,min_date,max_date,include_same_as=False,counts_only=True)
    return counts[0]
