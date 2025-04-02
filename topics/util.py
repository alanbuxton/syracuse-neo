import re
import string
from urllib.parse import urlparse
import hashlib
from datetime import datetime, timedelta, timezone

def cacheable_hash(input_string):
    hash_object = hashlib.sha256()
    hash_object.update(input_string.encode('utf-8'))
    return hash_object.hexdigest()

def cache_friendly(key):
    no_punct = re.sub(rf"[{string.punctuation} ]","_",key)
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
        min_date = datetime.fromisoformat(min_date)
    max_date = get_params.get("max_date",datetime.now(tz=timezone.utc))
    if isinstance(max_date, str):
        max_date = datetime.fromisoformat(max_date)
    if max_date is not None and min_date is None:
        min_date = max_date - timedelta(days=7)
    return min_date, max_date