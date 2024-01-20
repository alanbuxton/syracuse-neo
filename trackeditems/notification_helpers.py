from .serializers import ActivitySerializer
from topics.model_queries import get_activities_by_date_range_for_api
from .models import TrackedOrganization, get_notifiable_users
from datetime import datetime, timezone
from .date_helpers import days_ago
from django.template.loader import render_to_string

def prepare_recent_changes_email_notification(user, num_days):
    min_date = days_ago(num_days)
    max_date = datetime.now(tz=timezone.utc)
    orgs = TrackedOrganization.uris_by_user(user)
    matching_activity_orgs = get_activities_by_date_range_for_api(min_date, uri_or_list=orgs, max_date=max_date)
    if len(matching_activity_orgs) == 0:
        return None
    serializer = ActivitySerializer(matching_activity_orgs, many=True)
    merge_data = {"activities":serializer.data,"min_date":min_date,"day_count":num_days,
                    "max_date":max_date,"user":user,"tracked_orgs":orgs}
    html_body = render_to_string("activity_email_notif.html", merge_data)
    return html_body

def create_email_notifications(num_days=7):
    distinct_users = get_notifiable_users()
    for tracked_org_object in distinct_users:
        user = tracked_org_object.user
        email = prepare_recent_changes_email_notification(user, num_days)
        if email is not None:
            yield (user, email)
