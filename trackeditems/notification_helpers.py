from .serializers import ActivitySerializer, OrgIndGeoSerializer
from topics.activity_helpers import get_activities_by_org_uris_and_date_range, get_activities_by_industry_country_admin1_and_date_range
from trackeditems.models import get_notifiable_users, TrackedItem, ActivityNotification
from topics.models.model_helpers import similar_organizations_flat
from integration.models import DataImport
from topics.util import geo_to_country_admin1
from syracuse.date_util import min_and_max_date
from trackeditems.date_helpers import days_ago
from django.template.loader import render_to_string
from topics.models import Organization
from syracuse.cache_util import get_versionable_cache, set_versionable_cache
from topics.industry_geo.orgs_by_industry_geo import org_geo_industry_text_by_words
from django.core.cache import cache
import logging
logger = logging.getLogger(__name__)

def prepare_recent_changes_email_notification(user, num_days):
    max_date_for_email = DataImport.latest_import_ts()
    max_date = get_versionable_cache("activity_stats_last_updated")
    return prepare_recent_changes_email_notification_by_max_date(user, max_date, num_days, max_date_for_email)

def prepare_recent_changes_email_notification_by_max_date(user, max_date, num_days, max_date_for_email=None):
    if max_date_for_email is None:
        max_date_for_email = max_date
    min_date = ActivityNotification.most_recent(user)
    oldest_allowed_date = days_ago(num_days, max_date)
    if min_date is None or min_date < oldest_allowed_date:
        min_date = oldest_allowed_date
    return prepare_recent_changes_email_notification_by_min_max_date(user, min_date, max_date, max_date_for_email)

def recents_by_user_min_max_date(user, min_date, max_date):
    cache_key = f"recents_{user.id}_{min_date}_{max_date}"
    res = get_versionable_cache(cache_key)
    if res is not None:
        return res
    tracked_items = TrackedItem.trackable_by_user(user)
    matching_activity_orgs, serialized_data = tracked_items_between(tracked_items, min_date, max_date)
    set_versionable_cache(cache_key, (matching_activity_orgs, tracked_items), timeout=60*60 )
    return matching_activity_orgs, serialized_data

def tracked_items_between(tracked_items, min_date, max_date):
    org_uris = []
    matching_activity_orgs = []
    # min_date will always be less than 7 days
    seven_days_ago, max_date = min_and_max_date({"max_date":max_date},days_diff=7)
    assert seven_days_ago <= min_date, f"Expected {seven_days_ago} to be same or older than {min_date} (compared to {max_date})"

    for ti in tracked_items:
        if ti.organization_uri is not None:
            org = ti.organization_or_merged_org
            if org is not None:
                org_uris.append(org.uri)
                if ti.and_similar_orgs is True:
                    org_uris.extend(similar_organizations_flat(org,uris_only=True))
        elif ti.industry_search_str is not None:
            if ti.region is None:
                org_uris.extend(Organization.by_industry_text(ti.industry_search_str))
            else:
                org_uris.extend(org_geo_industry_text_by_words(ti.industry_search_str))
        elif (ti.industry_id is not None and ti.industry_id != '') or (ti.region is not None and ti.region != ''):
            country_code, admin1_code = geo_to_country_admin1(ti.region)
            acts = get_activities_by_industry_country_admin1_and_date_range(ti.industry_id,
                            country_code, admin1_code, seven_days_ago, max_date)
            matching_activity_orgs.extend(acts)
    org_activities = get_activities_by_org_uris_and_date_range(org_uris, seven_days_ago, max_date,limit=100)
    matching_activity_orgs.extend(org_activities)
    matching_activity_orgs = filter(lambda x: x["date_published"] >= min_date, matching_activity_orgs)
    matching_activity_orgs = sorted(matching_activity_orgs, key = lambda x: x['date_published'], reverse=True)
    serialized = OrgIndGeoSerializer(tracked_items,many=True)
    return matching_activity_orgs, serialized.data

def prepare_recent_changes_email_notification_by_min_max_date(user, min_date, max_date, max_date_for_email):
    matching_activity_orgs, tracked_items = recents_by_user_min_max_date(user, min_date, max_date)
    if len(matching_activity_orgs) == 0:
        return None
    return make_email_notif_from_orgs(matching_activity_orgs, tracked_items,
                                    min_date, max_date_for_email, user)

def make_email_notif_from_orgs(matching_activity_orgs, tracked_items,
                                min_date, max_date, user):
    activity_serializer = ActivitySerializer(matching_activity_orgs, many=True)
    merge_data = {"activities":activity_serializer.data,"min_date":min_date,
                    "max_date":max_date,"user":user,
                    "tracked_items": tracked_items,
                    "request_state": {"qs_params": {},},
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
