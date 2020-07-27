from functools import wraps
from genefab3.config import MAX_AUTOUPDATED_DATASETS, COLD_SEARCH_MASK
from genefab3.config import MAX_JSON_AGE, MAX_JSON_THREADS
from genefab3.utils import download_cold_json
from genefab3.exceptions import GeneLabJSONException
from genefab3.coldstoragedataset import ColdStorageDataset
from datetime import datetime
from pymongo import DESCENDING
from pandas import Series
from concurrent.futures import as_completed, ThreadPoolExecutor


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


def get_fresh_json(db, identifier, kind="other", max_age=MAX_JSON_AGE, compare=False):
    """Get JSON from local database if fresh, otherwise update local database and get"""
    json_cache_info = db.json_cache.find_one(
        {"identifier": identifier, "kind": kind},
        sort=[("last_refreshed", DESCENDING)],
    )
    if is_json_cache_fresh(json_cache_info, max_age):
        fresh_json, json_changed = json_cache_info["raw"], False
    else:
        try:
            fresh_json = download_cold_json(identifier, kind=kind)
        except Exception:
            try:
                fresh_json, json_changed = json_cache_info["raw"], False
            except (TypeError, KeyError):
                msg_mask = "Cannot retrieve cold storage JSON for '{}'"
                raise GeneLabJSONException(msg_mask.format(identifier))
        else:
            replace_doc(
                db.json_cache, {"identifier": identifier, "kind": kind},
                last_refreshed=int(datetime.now().timestamp()), raw=fresh_json,
            )
            if compare:
                json_changed = (fresh_json != json_cache_info.get("raw", {}))
    if compare:
        return fresh_json, json_changed
    else:
        return fresh_json


def refresh_dataset_json_store(db, accession):
    """Refresh top-level JSON of dataset in database"""
    glds_json, glds_changed = get_fresh_json(
        db, accession, "glds", compare=True,
    )
    replace_doc(
        db.dataset_timestamps, {"accession": accession},
        last_refreshed=int(datetime.now().timestamp()),
    )
    return glds_json, glds_changed


def get_dataset_with_caching(db, accession):
    """Refresh dataset JSONs in database"""
    glds_json, _ = refresh_dataset_json_store(db, accession)
    fileurls_json = get_fresh_json(db, accession, "fileurls")
    # internal _id is only found through dataset JSON, but may be cached:
    _id_search = db.accession_to_id.find_one({"accession": accession})
    if (_id_search is None) or ("cold_id" not in _id_search):
        # internal _id not cached, initialize dataset to find it:
        glds = ColdStorageDataset(
            accession, glds_json, fileurls_json, filedates_json=None,
        )
        replace_doc(
            db.accession_to_id, {"accession": accession}, cold_id=glds._id,
        )
        filedates_json = get_fresh_json(db, glds._id, "filedates")
    else:
        filedates_json = get_fresh_json(db, _id_search["cold_id"], "filedates")
        glds = ColdStorageDataset(
            accession, glds_json, fileurls_json, filedates_json,
        )
    return glds


def refresh_assay_property_store(db, assay):
    """Put per-sample, per-assay factors, annotation, and metadata into database"""
    for prop in "metadata", "annotation", "factors":
        db.assay_properties.delete_many({
            "accession": assay.dataset.accession,
            "assay_name": assay.name,
            "property": prop,
        })
        dataframe = getattr(assay, prop).full
        for sample_name, row in dataframe.iterrows():
            for (field, internal_field), value in row.iteritems():
                db.assay_properties.insert_one({
                    "accession": assay.dataset.accession,
                    "assay_name": assay.name,
                    "sample_name": sample_name,
                    "property": prop,
                    "field": field,
                    "internal_field": internal_field,
                    "value": value,
                })


def refresh_json_store_inner(db):
    """Iterate over datasets in cold storage, put updated JSONs into database"""
    fresh, stale = get_fresh_and_stale_accessions(db)
    try: # get number of datasets in database, and then all dataset JSONs
        n_datasets = min(
            get_fresh_json(db, COLD_SEARCH_MASK.format(0))["hits"]["total"],
            MAX_AUTOUPDATED_DATASETS,
        )
        url = COLD_SEARCH_MASK.format(n_datasets)
        raw_datasets_json = get_fresh_json(db, url)["hits"]["hits"]
        all_accessions = {raw_json["_id"] for raw_json in raw_datasets_json}
    except KeyError:
        raise GeneLabJSONException("Malformed search JSON")
    with ThreadPoolExecutor(max_workers=MAX_JSON_THREADS) as pool:
        future_to_accession = { # update stale JSONs
            pool.submit(refresh_dataset_json_store, db, accession): accession
            for accession in all_accessions - fresh
        }
        for future in as_completed(future_to_accession):
            _, glds_changed = future.result()
            accession = future_to_accession[future]
            if glds_changed:
                # TODO: use ThreadPoolExecutor
                glds = get_dataset_with_caching(db, accession)
                for assay in glds.assays.values():
                    refresh_assay_property_store(db, assay)
    for accession in (fresh | stale) - all_accessions: # drop removed datasets
        db.dataset_timestamps.delete_many({"accession": accession})
        db.accession_to_id.delete_many({"accession": accession})
    return all_accessions, fresh, stale


def refresh_json_store(db):
    """Keep all dataset and assay metadata up to date"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _ = refresh_json_store_inner(db)
            return func(*args, **kwargs)
        return wrapper
    return decorator
