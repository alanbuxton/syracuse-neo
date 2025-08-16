from syracuse.cache_util import get_versionable_cache
from datetime import date, datetime, time, timezone, timedelta

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
        max_date = get_versionable_cache("activity_stats_last_updated", version=cache_version)
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