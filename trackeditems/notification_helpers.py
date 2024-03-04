from .serializers import ActivitySerializer
from topics.model_queries import get_activities_by_date_range_for_api
from .models import TrackedOrganization, get_notifiable_users
from datetime import datetime, timezone
from .date_helpers import days_ago
from django.template.loader import render_to_string
from .models import ActivityNotification

def prepare_recent_changes_email_notification(user, num_days):
    max_date = datetime.now(tz=timezone.utc)
    return prepare_recent_changes_email_notification_by_max_date(user, max_date, num_days)

def prepare_recent_changes_email_notification_by_max_date(user, max_date, num_days):
    min_date = ActivityNotification.most_recent(user)
    if min_date is None:
        min_date = days_ago(num_days, max_date)
    return prepare_recent_changes_email_notification_by_min_max_date(user, min_date, max_date)

def prepare_recent_changes_email_notification_by_min_max_date(user, min_date, max_date):
    tracked_orgs = TrackedOrganization.by_user(user)
    org_uris = [x.organization_uri for x in tracked_orgs]
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, uri_or_list=org_uris, max_date=max_date)
    if len(matching_activity_orgs) == 0:
        return None
    serializer = ActivitySerializer(matching_activity_orgs, many=True)
    merge_data = {"activities":serializer.data,"min_date":min_date,
                    "max_date":max_date,"user":user,"tracked_orgs":tracked_orgs}
    html_body = render_to_string("activity_email_notif.html", merge_data)
    activity_notification = ActivityNotification(
        user = user,
        max_date = max_date,
        num_activities = len(matching_activity_orgs),
    )
    return html_body, activity_notification

def create_email_notifications(num_days=7):
    distinct_users = get_notifiable_users()
    for tracked_org_object in distinct_users:
        user = tracked_org_object.user
        email, activity_notification = prepare_recent_changes_email_notification(user, num_days)
        if email is not None:
            yield (user, email, activity_notification)
