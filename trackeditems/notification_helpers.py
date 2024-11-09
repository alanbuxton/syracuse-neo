from .serializers import ActivitySerializer
from topics.model_queries import (get_activities_by_date_range_for_api,
    get_activities_by_date_range_industry_geo_for_api)
from .models import TrackedOrganization, get_notifiable_users, TrackedIndustryGeo
from integration.models import DataImport
from .date_helpers import days_ago
from django.template.loader import render_to_string
from .models import ActivityNotification
from topics.views import industry_geo_search_str
from topics.geo_utils import geo_dict
from topics.serializers import IndustrySerializer
from topics.models import cache_friendly
from django.core.cache import cache

def prepare_recent_changes_email_notification(user, num_days):
    max_date = DataImport.latest_import_ts()
    return prepare_recent_changes_email_notification_by_max_date(user, max_date, num_days)

def prepare_recent_changes_email_notification_by_max_date(user, max_date, num_days):
    min_date = ActivityNotification.most_recent(user)
    if min_date is None:
        min_date = days_ago(num_days, max_date)
    return prepare_recent_changes_email_notification_by_min_max_date(user, min_date, max_date)

def recents_by_user_min_max_date(user, min_date, max_date):
    cache_key = cache_friendly(f"recents_{user.id}_{min_date}_{max_date}")
    res = cache.get(cache_key)
    if res is not None:
        return res
    tracked_orgs = TrackedOrganization.by_user(user)
    org_uris = [x.organization_or_merged_uri for x in tracked_orgs]
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, uri_or_list=org_uris, max_date=max_date,limit=100)
    tracked_industry_geos = []
    geo_lookup = geo_dict()
    for industry_name,geo_code in TrackedIndustryGeo.items_by_user(user):
        industry_id = IndustrySerializer(data={"industry":industry_name}).get_industry_id()
        acts = get_activities_by_date_range_industry_geo_for_api(min_date, max_date,geo_code,industry_id,limit=100)
        matching_activity_orgs.extend(acts)
        geo_name = geo_lookup[geo_code]
        tracked_industry_geos.append( industry_geo_search_str(industry_name, geo_name) )
    matching_activity_orgs = sorted(matching_activity_orgs, key = lambda x: x['date_published'], reverse=True)
    cache.set(cache_key, (matching_activity_orgs, tracked_orgs, tracked_industry_geos) )
    return matching_activity_orgs, tracked_orgs, tracked_industry_geos

def prepare_recent_changes_email_notification_by_min_max_date(user, min_date, max_date):
    matching_activity_orgs, tracked_orgs, tracked_industry_geos = recents_by_user_min_max_date(user, min_date, max_date)
    if len(matching_activity_orgs) == 0:
        return None
    return make_email_notif_from_orgs(matching_activity_orgs, tracked_orgs, tracked_industry_geos,
                                    min_date, max_date, user)

def make_email_notif_from_orgs(matching_activity_orgs, tracked_orgs, tracked_industry_geos,
                                min_date, max_date, user):
    serializer = ActivitySerializer(matching_activity_orgs, many=True)
    merge_data = {"activities":serializer.data,"min_date":min_date,
                    "max_date":max_date,"user":user,"tracked_orgs":tracked_orgs,
                    "tracked_industry_geos": tracked_industry_geos,
                    }
    html_body = render_to_string("activity_email_notif.html", merge_data)
    activity_notification = ActivityNotification(
        user = user,
        max_date = max_date,
        num_activities = len(matching_activity_orgs),
    )
    return html_body, activity_notification

def create_email_notifications(num_days=7):
    distinct_users = get_notifiable_users()
    for user in distinct_users:
        email_and_activity_notification = prepare_recent_changes_email_notification(user, num_days)
        if email_and_activity_notification is not None:
            email, activity_notification = email_and_activity_notification
            yield (user, email, activity_notification)
