from genefab3.config import MAX_JSON_AGE, COLLECTION_NAMES, COLD_SEARCH_MASK
from datetime import datetime
from pymongo import DESCENDING
from genefab3.coldstorage.json import download_cold_json
from genefab3.common.exceptions import GeneLabJSONException
from genefab3.backend.mongo.writers.metadata import run_mongo_transaction
from pandas import Series


def is_json_cache_fresh(json_cache_info, max_age=MAX_JSON_AGE):
    """Check if particular JSON cache is up to date"""
    if (json_cache_info is None) or ("raw" not in json_cache_info):
        return False
    else:
        current_timestamp = int(datetime.now().timestamp())
        cache_timestamp = json_cache_info.get("last_refreshed", -max_age)
        return (current_timestamp - cache_timestamp <= max_age)


def get_fresh_json(mongo_db, identifier, kind="other", max_age=MAX_JSON_AGE, report_changes=False, cname=COLLECTION_NAMES.JSON_CACHE):
    """Get JSON from local database if fresh, otherwise update local database and get"""
    json_cache_info = getattr(mongo_db, cname).find_one(
        {"identifier": identifier, "kind": kind},
        sort=[("last_refreshed", DESCENDING)],
    )
    if is_json_cache_fresh(json_cache_info, max_age):
        fresh_json, json_changed = json_cache_info["raw"], False
    else:
        try:
            fresh_json, _ = download_cold_json(identifier, kind=kind)
        except Exception:
            try:
                fresh_json, json_changed = json_cache_info["raw"], False
            except (TypeError, KeyError):
                raise GeneLabJSONException(
                    "Cannot retrieve cold storage JSON", identifier=identifier,
                )
        else:
            run_mongo_transaction(
                action="replace", collection=getattr(mongo_db, cname),
                query={"identifier": identifier, "kind": kind}, data={
                    "last_refreshed": int(datetime.now().timestamp()),
                    "raw": fresh_json,
                },
            )
            if report_changes and json_cache_info:
                json_changed = (fresh_json != json_cache_info.get("raw", {}))
            elif report_changes:
                json_changed = True
    if report_changes:
        return fresh_json, json_changed
    else:
        return fresh_json


def list_available_accessions(mongo_db):
    """List datasets in cold storage"""
    url_n = COLD_SEARCH_MASK.format(0)
    n_datasets = get_fresh_json(mongo_db, url_n)["hits"]["total"]
    url_all = COLD_SEARCH_MASK.format(n_datasets)
    raw_datasets_json = get_fresh_json(mongo_db, url_all)["hits"]["hits"]
    return {raw_json["_id"] for raw_json in raw_datasets_json}


def list_fresh_and_stale_accessions(mongo_db, max_age=MAX_JSON_AGE, cname=COLLECTION_NAMES.DATASET_TIMESTAMPS):
    """Find accessions in no need / need of update in database"""
    refresh_dates = Series({
        entry["accession"]: entry["last_refreshed"]
        for entry in getattr(mongo_db, cname).find()
    })
    current_timestamp = int(datetime.now().timestamp())
    indexer = ((current_timestamp - refresh_dates) <= max_age)
    return set(refresh_dates[indexer].index), set(refresh_dates[~indexer].index)


def drop_json_cache_by_accession(mongo_db, accession, cname=COLLECTION_NAMES.JSON_CACHE):
    """Remove all entries associated with dataset from db.json_cache"""
    run_mongo_transaction(
        action="delete_many", collection=getattr(mongo_db, cname),
        query={"identifier": accession},
    )
