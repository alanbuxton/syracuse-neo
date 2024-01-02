from datetime import datetime, timezone, timedelta

def days_ago(ago,d=datetime.now(tz=timezone.utc)):
    if isinstance(ago, str):
        ago = float(ago)
    return d - timedelta(days=ago)
