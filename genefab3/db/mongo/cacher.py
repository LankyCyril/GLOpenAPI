from threading import Thread
from genefab3.common.logger import GeneFabLogger
from time import sleep
from collections import OrderedDict
from genefab3.db.mongo.types import CachedDocumentByValue
from genefab3.isa.types import Dataset
from genefab3.db.mongo.utils import run_mongo_transaction, harmonize_document


class CacherThread(Thread):
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
 
    def __init__(self, *, adapter, mongo_db, sqlite_dbs, metadata_update_interval, metadata_retry_delay, units_formatter):
        """Prepare background thread that iteratively watches for changes to datasets"""
        self.adapter = adapter
        self.mongo_db, self.sqlite_dbs = mongo_db, sqlite_dbs
        self.metadata_update_interval = metadata_update_interval
        self.metadata_retry_delay = metadata_retry_delay
        self.units_formatter = units_formatter
        self.logger = GeneFabLogger()
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
            self.logger.info(f"CacherThread: Sleeping for {delay} seconds")
            sleep(delay)
 
    def recache_metadata(self):
        """Instantiate each available dataset; if contents changed, dataset automatically updates db.metadata"""
        self.logger.info("CacherThread: Checking metadata cache")
        try:
            accessions = OrderedDict(
                cached=set(self.mongo_db.metadata.distinct("info.accession")),
                live=set(self.adapter.get_accessions()),
                fresh=set(), updated=set(), dropped=set(), failed=set(),
            )
        except Exception as e:
            self.logger.error(f"CacherThread: {repr(e)}")
            return None, False
        for accession in accessions["cached"] - accessions["live"]:
            key, report = self.drop_single_dataset_metadata(accession)
            accessions[key].add(accession)
            self.logger.info(f"CacherThread/{accession}: {report}")
        for accession in accessions["live"]:
            key, report = self.recache_single_dataset_metadata(accession)
            accessions[key].add(accession)
            _func = self.logger.error if key == "failed" else self.logger.info
            _func(f"CacherThread/{accession}: {report}")
            # TODO: status, see genefab3/legacy/backend/mongo/dataset.py
        self.logger.info(
            "CacherThread, datasets: " + ", ".join(
                f"{k}={len(v)}" for k, v in accessions.items()
            ),
        )
        return accessions, True
 
    def drop_single_dataset_metadata(self, accession):
        """Drop all metadata entries associated with `accession` from `self.mongo_db.metadata`"""
        run_mongo_transaction(
            "delete_many", self.mongo_db.metadata, # TODO pass CNAME from Client
            query={"info.accession": accession},
        )
        return "dropped", "removed from database"
 
    def recache_single_dataset_metadata(self, accession):
        """Check if dataset changed, update metadata cached in `self.mongo_db.metadata`, report with result/errors"""
        files = CachedDocumentByValue(
            identifier=dict(kind="dataset files", accession=accession),
            collection=self.mongo_db.records, # TODO pass CNAME from Client
            value=self.adapter.get_files_by_accession(accession),
        )
        if files.changed:
            try:
                dataset = Dataset(accession, files.value, self.sqlite_dbs.blobs)
            except Exception as e:
                return "failed", f"failed to update ({repr(e)}), kept stale"
            else:
                self.drop_single_dataset_metadata(accession)
                for sample in dataset.samples:
                    self.mongo_db.metadata.insert_one(
                        harmonize_document(sample, self.units_formatter),
                    )
                return "updated", "updated"
        else:
            return "fresh", "no action (fresh)"
