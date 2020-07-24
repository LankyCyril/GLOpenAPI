from functools import wraps
from genefab3.config import COLD_API_ROOT, MAX_JSON_AGE
from datetime import datetime
from genefab3.json import download_cold_json
from genefab3.exceptions import GeneLabJSONException


def is_json_cache_fresh(json_cache_info, max_age=MAX_JSON_AGE):
    if (json_cache_info is None) or ("raw" not in json_cache_info):
        return False
    else:
        current_timestamp = int(datetime.now().timestamp())
        cache_timestamp = json_cache_info.get("timestamp", -max_age)
        return (current_timestamp - cache_timestamp <= max_age)


def get_fresh_json(db, identifier, kind="other", max_age=MAX_JSON_AGE):
    json_cache_info = db.json_cache.find_one({
        "identifier": identifier, "kind": kind,
        # TODO: grab latest in case stray ones found
    })
    if is_json_cache_fresh(json_cache_info, max_age):
        return json_cache_info["raw"]
    else:
        # TODO: remove stale json here
        json = download_cold_json(identifier, kind=kind)
        db.json_cache.insert_one({
            "identifier": identifier, "kind": kind,
            "timestamp": int(datetime.now().timestamp()),
            "raw": json,
        })
        return json


def refresh_json_store_inner(db):
    url = "{}/data/search/?term=GLDS&type=cgene&size=0".format(COLD_API_ROOT)
    try:
        n_datasets = get_fresh_json(db, url)["hits"]["total"]
    except KeyError:
        raise GeneLabJSONException("Malformed JSON: search (size=0)")
    return str(n_datasets)


def refresh_json_store(db):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            refresh_json_store_inner(db)
            return func(*args, **kwargs)
        return wrapper
    return decorator
