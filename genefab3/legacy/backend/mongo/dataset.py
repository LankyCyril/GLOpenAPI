from genefab3.config import COLLECTION_NAMES
from genefab3.coldstorage.dataset import ColdStorageDataset
from genefab3.backend.mongo.writers.json import get_fresh_json
from genefab3.backend.mongo.utils import run_mongo_transaction
from genefab3.backend.mongo.utils import harmonize_document
from datetime import datetime
from functools import partial


WARN_NO_META = "%s, %s: no metadata entries"
WARN_NO_STUDY = "%s, %s, %s: no Study entries"


class CachedDataset(ColdStorageDataset):
    """ColdStorageDataset via auto-updated metadata in database"""
 
    def __init__(self, mongo_db, accession, logger=None, init_assays=True, units_format=None, cname=COLLECTION_NAMES.DATASET_TIMESTAMPS):
        """Initialize with latest cached JSONs, recache if stale, init and recache stale assays if requested; drop cache on errors"""
        self.mongo_db = mongo_db
        try:
            super().__init__(
                accession, init_assays=False,
                get_json=partial(get_fresh_json, mongo_db=mongo_db),
                logger=logger,
            )
            if init_assays:
                self.init_assays()
                if any(self.changed):
                    for assay in self.assays.values():
                        self._recache_assay(assay, units_format=units_format)
            else:
                self.assays = CachedAssayDispatcher(self)
            run_mongo_transaction(
                action="replace", collection=getattr(mongo_db, cname),
                query={"accession": accession},
                data={"last_refreshed": int(datetime.now().timestamp())},
            )
        except Exception as e:
            self.drop_cache()
            self.update_status(accession=accession, status="failure", error=e)
            raise
        else:
            self.update_status(accession=accession, status="success")

    def update_status(self, accession, assay_name=None, status="success", warning=None, error=None, details=(), cname=COLLECTION_NAMES.STATUS):
        """Update status of dataset (and, optionally, assay) in db.status"""
        if assay_name is None:
            replacement_query = {"kind": "dataset", "accession": accession}
            if status == "failure":
                run_mongo_transaction(
                    action="delete_many",
                    collection=getattr(self.mongo_db, cname),
                    query={"accession": accession},
                )
        else:
            replacement_query = {
                "kind": "assay", "accession": accession,
                "assay name": assay_name,
            }
        inserted_data = {
            "status": status, "warning": warning,
            "error": None if (error is None) else type(error).__name__,
            "details": list(details),
            "report timestamp": int(datetime.now().timestamp())
        }
        if error is not None:
            inserted_data["details"].extend(getattr(error, "args", []))
        run_mongo_transaction(
            action="replace", collection=getattr(self.mongo_db, cname),
            query=replacement_query, data=inserted_data,
        )
 
    def _recache_assay(self, assay, units_format, cname=COLLECTION_NAMES.METADATA):
        """Recache assay metadata in db.metadata if any values in parent dataset JSON changed since last run"""
        metadata_collection = getattr(self.mongo_db, cname)
        run_mongo_transaction(
            action="delete_many", collection=metadata_collection,
            query={"info.accession": self.accession, "info.assay": assay.name},
        )
        if assay.metadata:
            run_mongo_transaction(
                action="insert_many", collection=metadata_collection,
                documents=harmonize_document(
                    assay.metadata.values(), units_format=units_format,
                ),
            )
            sample_names_with_missing_study_entries = set()
            for sample_name in assay.metadata:
                if "Study" not in assay.metadata[sample_name]:
                    sample_names_with_missing_study_entries.add(sample_name)
                    self.logger.warning(
                        WARN_NO_STUDY, self.accession, assay.name, sample_name,
                    )
            if sample_names_with_missing_study_entries:
                self.update_status(
                    accession=self.accession, assay_name=assay.name,
                    warning="Some Study entries missing", status="warning",
                    details=sorted(sample_names_with_missing_study_entries),
                )
            else:
                self.update_status(
                    accession=self.accession, assay_name=assay.name,
                )
        else:
            self.update_status(
                accession=self.accession, assay_name=assay.name,
                warning="No metadata entries", status="warning",
            )
            self.logger.warning(WARN_NO_META, self.accession, assay.name)


class CachedAssayDispatcher(dict):
    pass
