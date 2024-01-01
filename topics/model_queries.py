from neomodel import db
from .models import Organization, ActivityMixin
from datetime import datetime, timezone
from typing import List, Union

def get_activities_by_date_range_for_api(min_date, uri_or_list: Union[str,List[str]],
                                            max_date = datetime.now(tz=timezone.utc)):
    assert min_date is not None, "Must have min date"
    if uri_or_list is None or len(uri_or_list) == 0 or set(uri_or_list) == {None}:
        return []
    objs = get_activities_by_date_range(min_date, max_date, uri_or_list)
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

def get_activities_by_date_range(min_date, max_date, uri_or_uri_list: Union[str,List]):
    if isinstance(uri_or_uri_list, str):
        uri_list = [uri_or_uri_list]
    else:
        uri_list = uri_or_uri_list
    orgs = Organization.nodes.filter(uri__in=uri_list)
    uris_to_check = set(uri_list)
    for org in orgs:
        new_uris = [x.uri for x in org.same_as()]
        uris_to_check.update(new_uris)
    query = f"""
        match (n:CorporateFinanceActivity|RoleActivity|LocationActivity)--(o: Organization)
        where n.documentDate > datetime('{date_to_cypher_friendly(min_date)}')
        and n.documentDate < datetime('{date_to_cypher_friendly(max_date)}')
        and o.uri in {list(uris_to_check)}
        return *
    """
    objs, _ = db.cypher_query(query, resolve_objects=True)
    return objs # Each row is tuple of activity, organization

def date_to_cypher_friendly(date):
    if isinstance(date, str):
        return datetime.fromisoformat(date).isoformat()
    else:
        return date.isoformat()
