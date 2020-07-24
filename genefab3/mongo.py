from functools import wraps, partial
from genefab3.config import MAX_JSON_AGE, COLD_SEARCH_MASK
from datetime import datetime
from genefab3.utils import download_cold_json
from genefab3.exceptions import GeneLabJSONException
from pymongo import DESCENDING
from genefab3.coldstoragedataset import ColdStorageDataset as CSD
from pandas import Series


def replace_doc(db, query, **kwargs):
    """Shortcut to drop all instances and replace with updated instance"""
    db.delete_many(query)
    db.insert_one({**query, **kwargs})


def get_fresh_and_stale_accessions(db, max_age=MAX_JSON_AGE):
    """Find accessions in no need / need of update in database"""
    refresh_dates = Series({
        entry["accession"]: entry["last_refreshed"]
        for entry in db.dataset_timestamps.find()
    })
    current_timestamp = int(datetime.now().timestamp())
    indexer = ((current_timestamp - refresh_dates) <= max_age)
    return set(refresh_dates[indexer].index), set(refresh_dates[~indexer].index)


def is_json_cache_fresh(json_cache_info, max_age=MAX_JSON_AGE):
    """Check if particular JSON cache is up to date"""
    if (json_cache_info is None) or ("raw" not in json_cache_info):
        return False
    else:
        current_timestamp = int(datetime.now().timestamp())
        cache_timestamp = json_cache_info.get("last_refreshed", -max_age)
        return (current_timestamp - cache_timestamp <= max_age)


def get_fresh_json(db, identifier, kind="other", max_age=MAX_JSON_AGE):
    """Get JSON from local database if fresh, otherwise update local database and get"""
    json_cache_info = db.json_cache.find_one(
        {"identifier": identifier, "kind": kind},
        sort=[("last_refreshed", DESCENDING)],
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
            replace_doc(
                db.json_cache, {"identifier": identifier, "kind": kind},
                last_refreshed=int(datetime.now().timestamp()), raw=json,
            )
            return json


def refresh_json_store_inner(db):
    """Iterate over datasets in cold storage, put updated JSONs into database"""
    fresh, stale = get_fresh_and_stale_accessions(db)
    gfj = partial(get_fresh_json, db)
    try: # get number of datasets in database, and then all dataset JSONs
        url = COLD_SEARCH_MASK.format(0)
        n_datasets = gfj(url)["hits"]["total"]
        url = COLD_SEARCH_MASK.format(n_datasets)
        raw_datasets_json = gfj(url)["hits"]["hits"]
        all_accessions = {raw_json["_id"] for raw_json in raw_datasets_json}
    except KeyError:
        raise GeneLabJSONException("Malformed search JSON")
    for accession in all_accessions - fresh: # update stale JSONs
        glds_json, glds = gfj(accession, "glds"), None
        fileurls_json = gfj(accession, "fileurls")
        # internal _id is only found through dataset JSON, but may be cached:
        _id_search = db.accession_to_id.find_one({"accession": accession})
        if (_id_search is None) or ("cold_id" not in _id_search):
            # internal _id not cached, initialize dataset to find it:
            glds = CSD(accession, glds_json, fileurls_json, filedates_json=None)
            replace_doc(
                db.accession_to_id, {"accession": accession}, cold_id=glds._id,
            )
            filedates_json = gfj(glds._id, "filedates")
        else:
            filedates_json = gfj(_id_search["cold_id"], "filedates")
        if glds is None: # make sure ColdStorageDataset is initialized
            glds = CSD(accession, glds_json, fileurls_json, filedates_json)
        replace_doc(
            db.dataset_timestamps, {"accession": accession},
            last_refreshed=int(datetime.now().timestamp()),
        )
    for accession in (fresh | stale) - all_accessions: # drop removed datasets
        db.dataset_timestamps.delete_many({"accession": accession})
        db.accession_to_id.delete_many({"accession": accession})


def refresh_json_store(db):
    """Keep all dataset and assay metadata up to date"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            refresh_json_store_inner(db)
            return func(*args, **kwargs)
        return wrapper
    return decorator
