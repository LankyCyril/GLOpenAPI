from threading import Thread
from genefab3.common.logger import GeneFabLogger
from time import sleep
from collections import OrderedDict
from genefab3.db.mongo.types import ValueCheckedRecord
from genefab3.isa.types import Dataset
from genefab3.db.mongo.utils import run_mongo_transaction, harmonize_document
from genefab3.db.mongo.status import update_status


class MetadataCacherThread(Thread):
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
 
    def __init__(self, *, adapter, mongo_collections, sqlite_dbs, metadata_update_interval, metadata_retry_delay, units_formatter):
        """Prepare background thread that iteratively watches for changes to datasets"""
        self.adapter = adapter
        self.mongo_collections, self.sqlite_dbs = mongo_collections, sqlite_dbs
        self.metadata_update_interval = metadata_update_interval
        self.metadata_retry_delay = metadata_retry_delay
        self.units_formatter = units_formatter
        self.status_kwargs = dict(
            collection=self.mongo_collections.status,
            logger=GeneFabLogger(),
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
            GeneFabLogger().info(
                f"MetadataCacherThread: Sleeping for {delay} seconds",
            )
            sleep(delay)
 
    def recache_metadata(self):
        """Instantiate each available dataset; if contents changed, dataset automatically updates db.metadata"""
        GeneFabLogger().info("MetadataCacherThread: Checking metadata cache")
        try:
            accessions = OrderedDict(
                cached=set(
                    self.mongo_collections.metadata.distinct("info.accession"),
                ),
                live=set(self.adapter.get_accessions()),
                fresh=set(), updated=set(), dropped=set(), failed=set(),
            )
        except Exception as e:
            GeneFabLogger().error(f"MetadataCacherThread: {repr(e)}")
            return None, False
        def _iterate():
            for acc in accessions["cached"] - accessions["live"]:
                yield (acc, *self.drop_single_dataset_metadata(acc))
            for acc in accessions["live"]:
                yield (acc, *self.recache_single_dataset_metadata(acc))
        for accession, key, report, error in _iterate():
            accessions[key].add(accession)
            update_status(
                **self.status_kwargs, status=key, accession=accession,
                info=f"MetadataCacherThread: {accession} {report}", error=error,
            )
        GeneFabLogger().info(
            "MetadataCacherThread, datasets: " + ", ".join(
                f"{k}={len(v)}" for k, v in accessions.items()
            ),
        )
        return accessions, True
 
    def drop_single_dataset_metadata(self, accession):
        """Drop all metadata entries associated with `accession` from `self.mongo_collections.metadata`"""
        run_mongo_transaction(
            "delete_many", self.mongo_collections.metadata,
            query={"info.accession": accession},
        )
        return "dropped", "removed from database", None
 
    def recache_single_dataset_metadata(self, accession):
        """Check if dataset changed, update metadata cached in `self.mongo_collections.metadata`, report with result/errors"""
        files = ValueCheckedRecord(
            identifier=dict(kind="dataset files", accession=accession),
            collection=self.mongo_collections.records,
            value=self.adapter.get_files_by_accession(accession),
        )
        if files.changed:
            try:
                dataset = Dataset(
                    accession, files.value, self.sqlite_dbs.blobs,
                    sample_name_matcher=self.adapter.sample_name_matcher,
                    status_kwargs=self.status_kwargs,
                )
            except Exception as e:
                return "failed", f"failed to update ({repr(e)}), kept stale", e
            else:
                self.drop_single_dataset_metadata(accession)
                has_samples = False
                for sample in dataset.samples:
                    document = harmonize_document(sample, self.units_formatter)
                    self.mongo_collections.metadata.insert_one(document)
                    has_samples = True
                    if "Study" not in sample:
                        update_status(
                            **self.status_kwargs, status="warning",
                            warning="Study entry missing",
                            accession=accession, assay_name=sample.assay_name,
                            sample_name=sample.name,
                        )
                if not has_samples:
                    update_status(
                        **self.status_kwargs, status="warning",
                        warning="No samples", accession=accession,
                    )
                return "updated", "updated", None
        else:
            return "fresh", "no action (fresh)", None
