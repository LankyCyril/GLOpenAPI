from threading import Thread
from genefab3.common.logger import GeneFabLogger
from types import SimpleNamespace
from time import sleep
from collections import OrderedDict
from genefab3.db.mongo.types import CachedDocumentByValue
from genefab3.isa.types import Dataset
from genefab3.db.mongo.utils import run_mongo_transaction, harmonize_document
from functools import partial
from genefab3.db.mongo.status import update_status


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
 
    def __init__(self, *, adapter, mongo_db, sqlite_dbs, metadata_update_interval, metadata_retry_delay, units_formatter):
        """Prepare background thread that iteratively watches for changes to datasets"""
        self.adapter = adapter
        self.mongo_db, self.sqlite_dbs = mongo_db, sqlite_dbs
        self.metadata_update_interval = metadata_update_interval
        self.metadata_retry_delay = metadata_retry_delay
        self.units_formatter = units_formatter
        self.collections = SimpleNamespace( # TODO pass CNAMEs from Client
            metadata=self.mongo_db.metadata,
            records=self.mongo_db.records,
            status=self.mongo_db.status,
        )
        self._update_status = partial(
            update_status, collection=self.collections.status,
        )
        super().__init__()
 
    def run(self):
        """Continuously run MongoDB and SQLite3 cachers"""
        while True:
            # ensure_info_index TODO
            accessions, success = self.recache_metadata()
            if success:
                # update_metadata_value_lookup TODO
                # drop_cached_responses TODO
                # shrink_response_cache TODO
                delay = self.metadata_update_interval
            else:
                delay = self.metadata_retry_delay
            GeneFabLogger().info(f"CacherThread: Sleeping for {delay} seconds")
            sleep(delay)
 
    def recache_metadata(self):
        """Instantiate each available dataset; if contents changed, dataset automatically updates db.metadata"""
        GeneFabLogger().info("CacherThread: Checking metadata cache")
        try:
            accessions = OrderedDict(
                cached=set(self.mongo_db.metadata.distinct("info.accession")),
                live=set(self.adapter.get_accessions()),
                fresh=set(), updated=set(), dropped=set(), failed=set(),
            )
        except Exception as e:
            GeneFabLogger().error(f"CacherThread: {repr(e)}")
            return None, False
        def _iterate():
            for acc in accessions["cached"] - accessions["live"]:
                yield (acc, *self.drop_single_dataset_metadata(acc))
            for acc in accessions["live"]:
                yield (acc, *self.recache_single_dataset_metadata(acc))
        for accession, key, report, error in _iterate():
            accessions[key].add(accession)
            GeneFabLogger(
                call=("error" if key == "failed" else "info"),
                message=f"CacherThread: {accession} {report}",
                with_side_effects={
                    self._update_status: True, "status": key,
                    "accession": accession, "error": error,
                },
            )
        GeneFabLogger().info(
            "CacherThread, datasets: " + ", ".join(
                f"{k}={len(v)}" for k, v in accessions.items()
            ),
        )
        return accessions, True
 
    def drop_single_dataset_metadata(self, accession):
        """Drop all metadata entries associated with `accession` from `self.mongo_db.metadata`"""
        run_mongo_transaction(
            "delete_many", self.collections.metadata,
            query={"info.accession": accession},
        )
        return "dropped", "removed from database", None
 
    def recache_single_dataset_metadata(self, accession):
        """Check if dataset changed, update metadata cached in `self.mongo_db.metadata`, report with result/errors"""
        files = CachedDocumentByValue(
            identifier=dict(kind="dataset files", accession=accession),
            collection=self.collections.records,
            value=self.adapter.get_files_by_accession(accession),
        )
        if files.changed:
            try:
                dataset = Dataset(
                    accession, files.value, self.sqlite_dbs.blobs,
                    logger_kwargs={self._update_status: True},
                )
            except Exception as e:
                return "failed", f"failed to update ({repr(e)}), kept stale", e
            else:
                self.drop_single_dataset_metadata(accession)
                has_samples = False
                for sample in dataset.samples:
                    self.mongo_db.metadata.insert_one(
                        harmonize_document(sample, self.units_formatter),
                    )
                    has_samples = True
                    if "Study" not in sample:
                        GeneFabLogger(
                            call="warning", message="Study entry missing",
                            with_side_effects={
                                self._update_status: True, "status": "warning",
                                "accession": accession,
                                "sample_name": sample.name,
                            }
                        )
                if not has_samples:
                    GeneFabLogger(
                        call="warning", message="No samples",
                        with_side_effects={
                            self._update_status: True, "status": "warning",
                            "accession": accession,
                        }
                    )
                return "updated", "updated", None
        else:
            return "fresh", "no action (fresh)", None
