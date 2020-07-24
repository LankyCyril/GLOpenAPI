from functools import wraps, partial
from genefab3.config import COLD_API_ROOT, MAX_JSON_AGE
from datetime import datetime
from genefab3.json import download_cold_json
from genefab3.exceptions import GeneLabJSONException
from pymongo import DESCENDING
from genefab3.coldstoragedataset import ColdStorageDataset


def is_json_cache_fresh(json_cache_info, max_age=MAX_JSON_AGE):
    if (json_cache_info is None) or ("raw" not in json_cache_info):
        return False
    else:
        current_timestamp = int(datetime.now().timestamp())
        cache_timestamp = json_cache_info.get("timestamp", -max_age)
        return (current_timestamp - cache_timestamp <= max_age)


def get_fresh_json(db, identifier, kind="other", max_age=MAX_JSON_AGE):
    json_cache_info = db.json_cache.find_one(
        {"identifier": identifier, "kind": kind},
        sort=[("timestamp", DESCENDING)],
    )
    if is_json_cache_fresh(json_cache_info, max_age):
        return json_cache_info["raw"]
    else:
        try:
            json = download_cold_json(identifier, kind=kind)
        except Exception:
            try:
                return json_cache_info["raw"]
            except (TypeError, KeyError):
                raise GeneLabJSONException("Cannot retrieve cold storage JSON")
        else:
            db.json_cache.delete_many(
                {"identifier": identifier, "kind": kind},
            )
            db.json_cache.insert_one({
                "identifier": identifier, "kind": kind,
                "timestamp": int(datetime.now().timestamp()),
                "raw": json,
            })
            return json


def refresh_json_store_inner(db):
    """Iterate over datasets in cold storage, put updated JSONs into database"""
    gfj, dbatiio = partial(get_fresh_json, db), db.accession_to_id.insert_one
    CSD, glds = ColdStorageDataset, None
    search_url_mask = "{}/data/search/?term=GLDS&type=cgene&size={}"
    try: # get number of datasets in database, and then all dataset JSONs:
        url = search_url_mask.format(COLD_API_ROOT, 0)
        n_datasets = gfj(url)["hits"]["total"]
        url = search_url_mask.format(COLD_API_ROOT, n_datasets)
        raw_datasets_json = gfj(url)["hits"]["hits"]
    except KeyError:
        raise GeneLabJSONException("Malformed search JSON")
    for raw_json in raw_datasets_json:
        accession = raw_json["_id"]
        glds_json = gfj(accession, "glds")
        fileurls_json = gfj(accession, "fileurls")
        # internal _id is only found through dataset JSON, but may be cached:
        _id_search = db.accession_to_id.find_one({"accession": accession})
        if (_id_search is None) or ("_id" not in _id_search):
            # internal _id not cached, initialize dataset to find it:
            glds = CSD(accession, glds_json, fileurls_json, filedates_json=None)
            dbatiio({"accession": accession, "_id": glds._id})
            filedates_json = gfj(glds._id, "filedates")
        else:
            filedates_json = gfj(_id_search["_id"], "filedates")
        if glds is None: # make sure ColdStorageDataset is initialized
            glds = CSD(accession, glds_json, fileurls_json, filedates_json)


def refresh_json_store(db):
    """Keep all dataset and assay metadata up to date"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            refresh_json_store_inner(db)
            return func(*args, **kwargs)
        return wrapper
    return decorator
