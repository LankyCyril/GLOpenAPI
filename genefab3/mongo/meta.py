from logging import getLogger, INFO
from genefab3.config import COLD_SEARCH_MASK, MAX_JSON_AGE
from genefab3.config import CACHER_THREAD_CHECK_INTERVAL
from genefab3.config import CACHER_THREAD_RECHECK_DELAY
from genefab3.coldstorage.json import download_cold_json
from genefab3.mongo.utils import replace_doc, insert_one_safe
from genefab3.exceptions import GeneLabJSONException
from genefab3.coldstorage.dataset import ColdStorageDataset
from datetime import datetime
from pymongo import DESCENDING
from pandas import Series
from threading import Thread
from time import sleep
from functools import partial


def is_json_cache_fresh(json_cache_info, max_age=MAX_JSON_AGE):
    """Check if particular JSON cache is up to date"""
    if (json_cache_info is None) or ("raw" not in json_cache_info):
        return False
    else:
        current_timestamp = int(datetime.now().timestamp())
        cache_timestamp = json_cache_info.get("last_refreshed", -max_age)
        return (current_timestamp - cache_timestamp <= max_age)


def get_fresh_json(db, identifier, kind="other", max_age=MAX_JSON_AGE, report_changes=False):
    """Get JSON from local database if fresh, otherwise update local database and get"""
    json_cache_info = db.json_cache.find_one(
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
                msg_mask = "Cannot retrieve cold storage JSON for '{}'"
                raise GeneLabJSONException(msg_mask.format(identifier))
        else:
            replace_doc(
                db.json_cache, {"identifier": identifier, "kind": kind},
                last_refreshed=int(datetime.now().timestamp()), raw=fresh_json,
            )
            if report_changes and json_cache_info:
                json_changed = (fresh_json != json_cache_info.get("raw", {}))
            elif report_changes:
                json_changed = True
    if report_changes:
        return fresh_json, json_changed
    else:
        return fresh_json


def list_available_accessions(db):
    """List datasets in cold storage"""
    url_n = COLD_SEARCH_MASK.format(0)
    n_datasets = get_fresh_json(db, url_n)["hits"]["total"]
    url_all = COLD_SEARCH_MASK.format(n_datasets)
    raw_datasets_json = get_fresh_json(db, url_all)["hits"]["hits"]
    return {raw_json["_id"] for raw_json in raw_datasets_json}


def list_fresh_and_stale_accessions(db, max_age=MAX_JSON_AGE):
    """Find accessions in no need / need of update in database"""
    refresh_dates = Series({
        entry["accession"]: entry["last_refreshed"]
        for entry in db.dataset_timestamps.find()
    })
    current_timestamp = int(datetime.now().timestamp())
    indexer = ((current_timestamp - refresh_dates) <= max_age)
    return set(refresh_dates[indexer].index), set(refresh_dates[~indexer].index)


class CachedDataset(ColdStorageDataset):
    """ColdStorageDataset via auto-updated metadata in database"""
 
    def __init__(self, accession, db, init_assays=True):
        super().__init__(
            accession, init_assays=init_assays,
            get_json=partial(get_fresh_json, db=db),
        )
        replace_doc(
            db.dataset_timestamps, {"accession": accession},
            last_refreshed=int(datetime.now().timestamp()),
        )
        if init_assays:
            self.init_assays()
            if self.changed.glds:
                for assay_name, assay in self.assays.items():
                    for collection in db.metadata, db.annotations:
                        collection.delete_many({
                            ".Accession": accession, ".Assay": assay_name,
                        })
                    for entry in assay.metadata:
                        insert_one_safe(db.metadata, entry)
                    for entry in assay.annotation:
                        insert_one_safe(db.annotations, entry)


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache up to date"""
 
    def __init__(self, db, check_interval=CACHER_THREAD_CHECK_INTERVAL, recheck_delay=CACHER_THREAD_RECHECK_DELAY):
        self.db, self.check_interval = db, check_interval
        self.recheck_delay = recheck_delay
        self.logger = getLogger("genefab3")
        self.logger.setLevel(INFO)
        super().__init__()
 
    def run(self):
        while True:
            self.logger.info("CacherThread: Checking cache")
            try:
                accessions = list_available_accessions(self.db)
                fresh, stale = list_fresh_and_stale_accessions(self.db)
            except Exception as e:
                self.logger.error("CacherThread: %s", repr(e), stack_info=True)
                delay = self.recheck_delay
            else:
                for accession in accessions - fresh:
                    try:
                        glds = CachedDataset(accession, self.db)
                    except Exception as e:
                        self.logger.error(
                            "CacherThread: %s at accession %s",
                            repr(e), accession, stack_info=True,
                        )
                    else:
                        if any(glds.changed.__dict__.values()):
                            self.logger.info(
                                "CacherThread: %s changed", accession,
                            )
                for accession in (fresh | stale) - accessions:
                    self.db.dataset_timestamps.delete_many({
                        "accession": accession,
                    })
                self.logger.info(
                    "CacherThread: %d fresh, %d stale", len(fresh), len(stale),
                )
                delay = self.check_interval
            finally:
                self.logger.info("CacherThread: sleeping for %d seconds", delay)
                sleep(delay)
