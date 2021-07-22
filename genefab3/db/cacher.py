from threading import Thread
from genefab3.db.sql.response_cache import ResponseCache
from genefab3.common.exceptions import GeneFabLogger
from genefab3.db.mongo.index import ensure_info_index
from genefab3.db.mongo.index import update_metadata_value_lookup
from time import sleep
from collections import OrderedDict
from genefab3.db.mongo.utils import iterate_mongo_connections
from genefab3.db.mongo.types import ValueCheckedRecord
from genefab3.isa.types import Dataset
from genefab3.db.mongo.utils import run_mongo_action, harmonize_document
from genefab3.db.mongo.status import drop_status, update_status


class MetadataCacherThread(Thread):
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
    CLIENT_ATTRIBUTES_TO_COPY = (
      "adapter", "mongo_collections", "locale", "sqlite_dbs", "units_formatter",
    )
 
    def __init__(self, *, genefab3_client, full_update_interval, full_update_retry_delay, dataset_init_interval, dataset_update_interval):
        """Prepare background thread that iteratively watches for changes to datasets"""
        self.genefab3_client = genefab3_client
        self._id = genefab3_client.mongo_appname.replace(
            "GeneFab3", "MetadataCacherThread",
        )
        for attr in self.CLIENT_ATTRIBUTES_TO_COPY:
            setattr(self, attr, getattr(genefab3_client, attr))
        self.full_update_interval = full_update_interval
        self.full_update_retry_delay = full_update_retry_delay
        self.dataset_init_interval = dataset_init_interval
        self.dataset_update_interval = dataset_update_interval
        self.response_cache = ResponseCache(self.sqlite_dbs)
        self.status_kwargs = dict(collection=self.mongo_collections.status)
        super().__init__()
 
    def run(self):
        """Continuously run MongoDB and SQLite3 cachers"""
        while True:
            ensure_info_index(self.mongo_collections, self.locale)
            accessions, success = self.recache_metadata()
            if success:
                update_metadata_value_lookup(self.mongo_collections, self._id)
                if accessions["updated"]:
                    self.response_cache.drop_all()
                else:
                    for acc in accessions["failed"] | accessions["dropped"]:
                        self.response_cache.drop(acc)
                self.response_cache.shrink()
                delay = self.full_update_interval
            else:
                delay = self.full_update_retry_delay
            GeneFabLogger.info(f"{self._id}:\n  Sleeping for {delay} seconds")
            sleep(delay)
 
    def get_accession_dispatcher(self):
        """Return dict of sets of accessions; populates: 'cached', 'live'; prepares empty: 'fresh', 'updated', 'stale', 'dropped', 'failed'"""
        GeneFabLogger.info(f"{self._id}:\n  Checking metadata cache")
        collection = self.mongo_collections.metadata
        return OrderedDict(
            cached=set(collection.distinct("id.accession")),
            live=set(self.adapter.get_accessions()), fresh=set(),
            updated=set(), stale=set(), dropped=set(), failed=set(),
        )
 
    def recache_metadata(self):
        """Instantiate each available dataset; if contents changed, dataset automatically updates db.metadata"""
        try:
            accessions = self.get_accession_dispatcher()
        except Exception as e:
            GeneFabLogger.error(f"{self._id}:\n  {e!r}", exc_info=e)
            return None, False
        def _iterate_with_delay():
            for a in accessions["cached"] - accessions["live"]:
                yield (a, *self.drop_single_dataset_metadata(a))
            for a in accessions["live"]:
                has_cache = a in accessions["cached"]
                if has_cache:
                    delay, _r = self.dataset_update_interval, "updating cached"
                else:
                    delay, _r = self.dataset_init_interval, "retrieving new"
                msg = f"Sleeping {delay} seconds before {_r} dataset {a}"
                GeneFabLogger.debug(f"{self._id}, {msg}")
                sleep(delay)
                yield (a, *self.recache_single_dataset_metadata(a, has_cache))
        for accession, key, report, error in _iterate_with_delay():
            accessions[key].add(accession)
            _kws = dict(
                **self.status_kwargs, status=key, accession=accession,
                prefix=self._id, info=f"{accession} {report}", error=error,
            )
            if key in {"dropped", "failed"}:
                drop_status(**_kws)
            update_status(**_kws)
            mongo_client = self.genefab3_client.mongo_client
            n_apps = sum(1 for _ in iterate_mongo_connections(mongo_client))
            msg = f"Total number of active MongoDB connections: {n_apps}"
            GeneFabLogger.info(msg)
        GeneFabLogger.info(f"{self._id}, datasets:\n  " + ", ".join(
            f"{k}={len(v)}" for k, v in accessions.items()
        ))
        return accessions, True
 
    def drop_single_dataset_metadata(self, accession):
        """Drop all metadata entries associated with `accession` from `self.mongo_collections.metadata`"""
        collection = self.mongo_collections.metadata
        with collection.database.client.start_session() as session:
            with session.start_transaction():
                run_mongo_action(
                    "delete_many", collection,
                    query={"id.accession": accession},
                )
        return "dropped", "removed from database", None
 
    def recache_single_dataset_samples(self, dataset):
        """Insert per-sample documents into MongoDB, return exception on error"""
        collection = self.mongo_collections.metadata
        with collection.database.client.start_session() as session:
            with session.start_transaction():
                try:
                    has_samples = False
                    for sample in dataset.samples:
                        collection.insert_one(harmonize_document(
                            sample, self.units_formatter,
                        ))
                        has_samples = True
                        if "Study" not in sample:
                            update_status(
                                **self.status_kwargs, status="warning",
                                warning="Study entry missing",
                                accession=dataset.accession,
                                assay_name=sample.assay_name,
                                sample_name=sample.name,
                            )
                    if not has_samples:
                        update_status(
                            **self.status_kwargs, status="warning",
                            warning="No samples", accession=dataset.accession,
                        )
                except Exception as e:
                    msg = f"{self._id} @ {dataset.accession} samples:\n  {e!r}"
                    GeneFabLogger.error(msg, exc_info=e)
                    return e
                else:
                    return None
 
    def recache_single_dataset_metadata(self, accession, has_cache):
        """Check if dataset changed, update metadata cached in `self.mongo_collections.metadata`, report with result/errors"""
        try:
            files = ValueCheckedRecord(
                identifier=dict(kind="dataset files", accession=accession),
                collection=self.mongo_collections.records,
                value=self.adapter.get_files_by_accession(accession),
            )
            if files.changed or (not has_cache):
                best_sample_name_matches=self.adapter.best_sample_name_matches
                dataset = Dataset(
                    accession, files.value, self.sqlite_dbs,
                    best_sample_name_matches=best_sample_name_matches,
                    status_kwargs=self.status_kwargs,
                )
            else:
                dataset = None
        except Exception as e:
            msg = f"{self._id} @ {accession}:\n  {e!r}"
            if has_cache:
                status = "stale"
                report = f"failed to retrieve ({repr(e)}), kept stale"
                GeneFabLogger.warning(msg, exc_info=e)
            else:
                status, report = "failed", f"failed to retrieve ({repr(e)})"
                GeneFabLogger.error(msg, exc_info=e)
            return status, report, e
        if dataset is not None: # files have changed OR needs to be re-inserted
            self.drop_single_dataset_metadata(accession)
            e = self.recache_single_dataset_samples(dataset)
            if e is not None:
                self.drop_single_dataset_metadata(accession)
                return "failed", f"failed to parse ({repr(e)})", e
            else:
                return "updated", "updated", None
        else: # files have not changed
            return "fresh", "no action (fresh)", None
