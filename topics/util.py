import re
import string
from urllib.parse import urlparse
import hashlib
from datetime import datetime, timedelta, time, date
from django.core.cache import cache

def cacheable_hash(input_string):
    hash_object = hashlib.sha256()
    hash_object.update(input_string.encode('utf-8'))
    return hash_object.hexdigest()

def clean_punct(text,replacement=' '):
    return re.sub(rf"[{string.punctuation} ]",replacement,text)

def cache_friendly(key):
    no_punct = clean_punct(key,"_")
    cleaned = re.sub(r"_{2,}","_",no_punct)
    if len(cleaned) > 230:
        cleaned = cleaned[:180] + str(cacheable_hash(cleaned[180:]))
    return cleaned

def blank_or_none(val):
    if val is None:
        return True
    if isinstance(val, str) and val.strip() == '':
        return True
    return False

def geo_to_country_admin1(geo_code):
    if geo_code is None: 
        return None, None
    splitted = geo_code.split("-")
    country_code = splitted[0]
    admin1_code = splitted[1] if len(splitted) > 1 else None
    return country_code, admin1_code

def elements_from_uri(uri):
    if uri is None or uri == '':
        return {}
    parsed = urlparse(uri)
    part_pieces = parsed.path.split("/")
    path = part_pieces[1]
    doc_id = part_pieces[2]
    org_name = "/".join(part_pieces[3:])
    return {
        "domain": parsed.netloc,
        "path": path,
        "doc_id": doc_id,
        "name": org_name,
    }


def min_and_max_date(get_params):
    min_date = get_params.get("min_date")
    if isinstance(min_date, str):
        min_date = date.fromisoformat(min_date)
    max_date = get_params.get("max_date")
    if isinstance(max_date, str):
        max_date = date.fromisoformat(max_date)
    max_date = end_of_day(max_date)
    if max_date is None:
        max_date = cache.get("activity_stats_last_updated")
    if max_date is not None and min_date is None:
        min_date = max_date - timedelta(days=7)
    min_date = start_of_day(min_date)
    return min_date, max_date

def end_of_day(d):
    if d is None:
        return None
    return datetime.combine(d, time.max)

def start_of_day(d):
    return datetime.combine(d, time.min)

def camel_case_to_snake_case(text):
    text = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', text))
    text = re.sub(r'\s+','_', text)
    return text.lower()
