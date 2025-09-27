from django.core.cache import cache
import hashlib
import redis
from django_redis import get_redis_connection

import logging
logger = logging.getLogger(__name__)

ACTIVE_VERSION_KEY = "versionable:active_version"

VERSIONS = ["castor","pollux"]

def get_active_version():
    return cache.get(ACTIVE_VERSION_KEY, VERSIONS[0])

def set_active_version(version):
    cache.set(ACTIVE_VERSION_KEY, version)
    delete_previous_version()

def get_inactive_version():
    return  VERSIONS[1] if get_active_version() == VERSIONS[0] else VERSIONS[0]

def set_versionable_cache(cache_key, val, version=None, **kwargs):
    if version is None:
        version = get_active_version()
    assert version in VERSIONS, f"Expected version {version} to be one of {VERSIONS}"
    key = cache_friendly(f"{version}_{cache_key}")
    cache.set(key, val, **kwargs)
    return key

def get_versionable_cache(cache_key, version=None):
    if version is None:
        version = get_active_version()
    assert version in VERSIONS, f"Expected version {version} to be one of {VERSIONS}"
    key = cache_friendly(f"{version}_{cache_key}")
    return cache.get(key)

def cache_friendly(key):
    cleaned = key + ""
    if len(cleaned) > 230:
        cleaned = cleaned[:180] + str(cacheable_hash(cleaned[180:]))
    return cleaned

def cacheable_hash(input_string):
    hash_object = hashlib.sha256()
    hash_object.update(input_string.encode('utf-8'))
    return hash_object.hexdigest()

def delete_previous_version():
    cache.delete_pattern(f"{get_inactive_version()}_*")

def nuke_cache():
    r = redis.Redis()
    r.flushdb()


def count_keys(pattern: str, cache_alias: str = "default", chunk_size: int = 1000) -> int:
    """
    Count Redis keys matching a given pattern.

    Uses SCAN to avoid blocking like KEYS would.

    Args:
        pattern (str): Redis glob-style pattern, e.g. "*foo*" 
        cache_alias (str): Django cache alias (default: "default")
        chunk_size (int): Number of keys to fetch per scan step

    Returns:
        int: Number of matching keys
    """
    redis_client = get_redis_connection(cache_alias)
    cursor = 0
    total = 0

    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=chunk_size)
        total += len(keys)
        if cursor == 0:
            break

    return total

def delete_keys_pattern_pipeline(pattern: str, cache_alias: str = "default", batch_size=1000):
    '''
        pattern can be e.g. "*foo_*"
    '''
    redis_client = get_redis_connection(cache_alias)
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=batch_size)
        if keys:
            pipe = redis_client.pipeline()
            for key in keys:
                pipe.delete(key)
            pipe.execute()
        if cursor == 0:
            break