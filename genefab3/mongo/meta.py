from os import environ
from sys import stderr
from genefab3.config import COLD_SEARCH_MASK, MAX_JSON_AGE
from genefab3.config import CACHER_THREAD_CHECK_INTERVAL
from genefab3.config import CACHER_THREAD_RECHECK_INTERVAL
from genefab3.config import ASSAY_METADATALIKES
from genefab3.coldstorage.json import download_cold_json
from genefab3.mongo.utils import replace_doc, insert_one_safe
from genefab3.exceptions import GeneLabJSONException
from genefab3.coldstorage.dataset import ColdStorageDataset
from datetime import datetime
from pymongo import DESCENDING
from pandas import Series
from threading import Thread
from time import sleep


DEBUG = (environ.get("FLASK_ENV", None) == "development")


def cacher_thread_log(message, error=False):
    """Log message about CacherThread"""
    if DEBUG:
        if error:
            print_mask = "CacherThread ERROR @ {}: {}"
        else:
            print_mask = "CacherThread message @ {}: {}"
        print(print_mask.format(datetime.now(), message), file=stderr)


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
            db.accession_to_id, {"accession": accession}, cold_id=glds.isa._id,
        )
        filedates_json = get_fresh_json(db, glds.isa._id, "filedates")
    else:
        filedates_json = get_fresh_json(db, _id_search["cold_id"], "filedates")
        glds = ColdStorageDataset(
            accession, glds_json, fileurls_json, filedates_json,
        )
    return glds


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
                insert_one_safe(collection, {
                    **{
                        "accession": assay.dataset.accession,
                        "assay name": assay.name, "sample name": sample_name,
                    },
                    **row.groupby(row.index).aggregate(list).to_dict(),
                })


def refresh_database_metadata_for_some_datasets(db, accessions):
    """Put updated JSONs for datasets with {accessions} and their assays into database"""
    datasets_with_updated_assays = []
    for accession in accessions:
        _, glds_changed = refresh_dataset_json_store(db, accession)
        cacher_thread_log("Refreshed JSON for dataset {}".format(accession))
        if glds_changed:
            cacher_thread_log("JSON changed for dataset {}".format(accession))
            datasets_with_updated_assays.append(accession)
            refresh_assay_meta_stores(db, accession)
            cacher_thread_log(
                "Refreshed JSON for assays in {}".format(accession),
            )
    return datasets_with_updated_assays


def refresh_database_metadata(db):
    """Iterate over datasets in cold storage, put updated JSONs into database"""
    fresh, stale = get_fresh_and_stale_accessions(db)
    try: # get number of datasets in database, and then all dataset JSONs
        url_n = COLD_SEARCH_MASK.format(0)
        n_datasets = get_fresh_json(db, url_n)["hits"]["total"]
        url_all = COLD_SEARCH_MASK.format(n_datasets)
        raw_datasets_json = get_fresh_json(db, url_all)["hits"]["hits"]
        all_accessions = {raw_json["_id"] for raw_json in raw_datasets_json}
    except KeyError:
        cacher_thread_log(
            "Cold storage returned malformed search JSON", error=True,
        )
    else:
        updated_assays = refresh_database_metadata_for_some_datasets(
            db, all_accessions - fresh,
        )
        for accession in (fresh | stale) - all_accessions:
            # drop removed datasets:
            db.dataset_timestamps.delete_many({"accession": accession})
            db.accession_to_id.delete_many({"accession": accession})
        return all_accessions, fresh, stale, updated_assays


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache up to date"""
    def __init__(self, db, check_interval=CACHER_THREAD_CHECK_INTERVAL, recheck_interval=CACHER_THREAD_RECHECK_INTERVAL):
        self.db, self.check_interval = db, check_interval
        self.recheck_interval = recheck_interval
        super().__init__()
    def run(self):
        while True:
            cacher_thread_log("Checking cache")
            try:
                accessions, fresh, stale, _ = refresh_database_metadata(self.db)
            except Exception as e:
                cacher_thread_log("{}".format(e), error=True)
                cacher_thread_log("Will try again after {} seconds".format(
                    self.recheck_interval
                ))
                sleep(self.recheck_interval)
            else:
                cacher_thread_log("{} fresh, {} stale accessions".format(
                    len(fresh), len(stale),
                ))
                cacher_thread_log("Will now sleep for {} seconds".format(
                    self.check_interval
                ))
                sleep(self.check_interval)
