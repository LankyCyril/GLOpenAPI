from genefab3.config import COLD_SEARCH_MASK, MAX_JSON_AGE
from genefab3.mongo.json import get_fresh_json
from datetime import datetime
from pandas import Series
from copy import deepcopy
from genefab3.mongo.utils import replace_doc
from logging import getLogger, DEBUG
from threading import Thread
from genefab3.config import CACHER_THREAD_CHECK_INTERVAL
from genefab3.config import CACHER_THREAD_RECHECK_DELAY
from genefab3.mongo.dataset import CachedDataset
from time import sleep


INDEX_TEMPLATE = {
    "investigation": {
        "study": "true",
        "study assays": "true",
        "investigation": "true",
    },
    "study": {
        "characteristics": "true",
        "factor value": "true",
        "parameter value": "true",
    },
    "assay": {
        "characteristics": "true",
        "factor value": "true",
        "parameter value": "true",
    },
}

FINAL_INDEX_KEY_BLACKLIST = {"comment"}


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


def INPLACE_update_metadata_index_keys(index, metadata, final_key_blacklist=FINAL_INDEX_KEY_BLACKLIST):
    """Populate JSON with all possible metadata keys, also for documentation section 'meta-existence'"""
    for isa_category in index:
        for subkey in index[isa_category]:
            raw_next_level_keyset = set.union(*(
                set(entry[isa_category][subkey].keys()) for entry in
                metadata.find(
                    {isa_category+"."+subkey: {"$exists": True}},
                    {isa_category+"."+subkey: True},
                )
            ))
            index[isa_category][subkey] = {
                next_level_key: True for next_level_key in
                sorted(raw_next_level_keyset - final_key_blacklist)
            }


def INPLACE_update_metadata_index_values(index, metadata):
    """Generate JSON with all possible metadata values, also for documentation section 'meta-equals'"""
    for isa_category in index:
        for subkey in index[isa_category]:
            for next_level_key in index[isa_category][subkey]:
                index[isa_category][subkey][next_level_key] = sorted(
                    map(str, metadata.distinct(
                        f"{isa_category}.{subkey}.{next_level_key}.",
                    ))
                )


def update_metadata_index(db, template=INDEX_TEMPLATE):
    """Collect existing keys and values for lookups"""
    index = deepcopy(template)
    INPLACE_update_metadata_index_keys(index, db.metadata)
    INPLACE_update_metadata_index_values(index, db.metadata)
    for isa_category in index:
        for subkey in index[isa_category]:
            try:
                replace_doc(
                    collection=db.metadata_index,
                    query={"isa_category": isa_category, "subkey": subkey},
                    doc={"content": index[isa_category][subkey]},
                )
            except:
                print(index[isa_category][subkey])
                raise


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache and index up to date"""
 
    def __init__(self, db, check_interval=CACHER_THREAD_CHECK_INTERVAL, recheck_delay=CACHER_THREAD_RECHECK_DELAY):
        self.db, self.check_interval = db, check_interval
        self.recheck_delay = recheck_delay
        self.logger = getLogger("genefab3")
        self.logger.setLevel(DEBUG)
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
                        glds = CachedDataset(self.db, accession, self.logger)
                    except Exception as e:
                        self.logger.error(
                            "CacherThread: %s at accession %s",
                            repr(e), accession, stack_info=True,
                        )
                    else:
                        if any(glds.changed.__dict__.values()):
                            chg = "changed"
                        else:
                            chg = "up to date"
                        self.logger.info("CacherThread: %s %s", accession, chg)
                for accession in (fresh | stale) - accessions:
                    CachedDataset.drop_cache(db=self.db, accession=accession)
                self.logger.info(
                    "CacherThread: %d fresh, %d stale", len(fresh), len(stale),
                )
                delay = self.check_interval
            finally:
                self.logger.info("CacherThread: reindexing metadata")
                update_metadata_index(self.db)
                self.logger.info("CacherThread: sleeping for %d seconds", delay)
                sleep(delay)
