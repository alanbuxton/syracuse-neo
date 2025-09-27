from syracuse.cache_util import get_versionable_cache
from datetime import date, datetime, time, timezone, timedelta

def latest_cache_date(cache_version=None):
    return get_versionable_cache("activity_stats_last_updated", version=cache_version)

def min_and_max_date_based_on_days_ago(days_ago):
    if days_ago is None:
        days_ago = 90
    days_ago = int(days_ago)
    if days_ago not in [7,30]:
        days_ago = 90
    return min_and_max_date({}, days_diff=days_ago)

def min_date_from_date(max_date=None, days_diff=90, cache_version=None):
    params = {}
    if max_date:
        params["max_date"]=max_date
    return min_and_max_date(params, days_diff, cache_version)

def min_and_max_date(get_params, days_diff=7, cache_version=None):
    min_date = get_params.get("min_date")
    if isinstance(min_date, str):
        min_date = date.fromisoformat(min_date)
    max_date = get_params.get("max_date")
    if isinstance(max_date, str):
        max_date = date.fromisoformat(max_date)
    max_date = end_of_day(max_date)
    if max_date and max_date.tzinfo is None:
        max_date = max_date.replace(tzinfo=timezone.utc)
    if max_date is None:
        max_date = latest_cache_date(cache_version=cache_version)
    if max_date is None:
        raise ValueError(f"max_date is still None from get_params {get_params}")
    if max_date is not None and min_date is None:
        min_date = max_date - timedelta(days=days_diff)
    min_date = start_of_day(min_date)
    return min_date, max_date

def end_of_day(d):
    if d is None:
        return None
    combined = datetime.combine(d, time.max)
    if combined.tzinfo is None:
        combined = combined.replace(tzinfo=timezone.utc)
    return combined

def start_of_day(d):
    combined = datetime.combine(d, time.min)
    if combined.tzinfo is None:
        combined = combined.replace(tzinfo=timezone.utc)
    return combined

def date_minus(to_date, days):
    prev_date =  to_date - timedelta(days=days)
    return start_of_day(prev_date)