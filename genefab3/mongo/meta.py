from os import environ
from sys import stderr
from genefab3.config import MAX_AUTOUPDATED_DATASETS, COLD_SEARCH_MASK
from genefab3.config import MAX_JSON_AGE, MAX_JSON_THREADS
from genefab3.config import ASSAY_METADATALIKES
from genefab3.utils import download_cold_json
from genefab3.mongo.utils import replace_doc
from genefab3.exceptions import GeneLabJSONException
from genefab3.coldstorage.dataset import ColdStorageDataset
from datetime import datetime
from pymongo import DESCENDING
from pandas import Series
from concurrent.futures import as_completed, ThreadPoolExecutor


DEBUG = (environ.get("FLASK_ENV", None) == "development")


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
            if compare and json_cache_info:
                json_changed = (fresh_json != json_cache_info.get("raw", {}))
            elif compare:
                json_changed = True
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


def refresh_many_datasets(db, accessions, max_workers=MAX_JSON_THREADS):
    """Update stale GLDS JSONs and database entries"""
    datasets_with_assays_to_update = []
    with ThreadPoolExecutor(max_workers=MAX_JSON_THREADS) as pool:
        future_to_accession = {
            pool.submit(refresh_dataset_json_store, db, accession): accession
            for accession in accessions
        }
        for future in as_completed(future_to_accession):
            _, glds_changed = future.result()
            accession = future_to_accession[future]
            if DEBUG:
                print("Refreshed JSON for dataset:", accession, file=stderr)
            if glds_changed:
                print("JSON changed for dataset:", accession, file=stderr)
                datasets_with_assays_to_update.append(accession)
    return datasets_with_assays_to_update


def refresh_assay_meta_stores(db, accession):
    """Put per-sample, per-assay factors, annotation, and metadata into database"""
    glds = get_dataset_with_caching(db, accession)
    for assay in glds.assays.values():
        for meta in ASSAY_METADATALIKES:
            collection = getattr(db, meta)
            dataframe = getattr(assay, meta).named
            collection.delete_many({
                "accession": assay.dataset.accession, "assay name": assay.name,
            })
            for sample_name, row in dataframe.iterrows():
                for field, value in row.iteritems():
                    collection.insert_one({
                        "accession": assay.dataset.accession,
                        "assay name": assay.name,
                        "sample name": sample_name, field: value,
                    })


def refresh_many_assays(db, datasets_with_assays_to_update, max_workers=MAX_JSON_THREADS):
    """Update stale assay JSONs and db entries"""
    with ThreadPoolExecutor(max_workers=MAX_JSON_THREADS) as pool:
        future_to_accession = {
            pool.submit(refresh_assay_meta_stores, db, accession):
            accession for accession in datasets_with_assays_to_update
        }
        for future in as_completed(future_to_accession):
            if DEBUG:
                acc = future_to_accession[future]
                print("Refreshed JSON for assays in:", acc, file=stderr)


def refresh_database_metadata_for_some_datasets(db, accessions):
    """Put updated JSONs for datasets with {accessions} and their assays into database"""
    datasets_with_assays_to_update = refresh_many_datasets(
        db, accessions, max_workers=MAX_JSON_THREADS,
    )
    refresh_many_assays(
        db, datasets_with_assays_to_update, max_workers=MAX_JSON_THREADS,
    )
    return datasets_with_assays_to_update


def refresh_database_metadata_for_one_dataset(db, accession):
    """Put updated JSONs for one dataset and its assays into database"""
    return refresh_database_metadata_for_some_datasets(db, {accession})


def refresh_database_metadata_for_all_datasets(db):
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
    updated_assays = refresh_database_metadata_for_some_datasets(
        db, all_accessions - fresh,
    )
    for accession in (fresh | stale) - all_accessions: # drop removed datasets
        db.dataset_timestamps.delete_many({"accession": accession})
        db.accession_to_id.delete_many({"accession": accession})
    return all_accessions, fresh, stale, updated_assays


def refresh_database_metadata(db, context_select=None):
    if context_select is None:
        return refresh_database_metadata_for_all_datasets(db)
    else:
        refresh_database_metadata_for_some_datasets(db, set(context_select))
        return None
