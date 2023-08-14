from time import sleep
from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.db.mongo.index import ensure_indices
from collections import OrderedDict
from glopenapi.common.hacks import apply_hack, convert_legacy_metadata_pre
from glopenapi.common.hacks import convert_legacy_metadata_post
from glopenapi.common.hacks import precache_metadata_counts
from glopenapi.common.hacks import TEMP_force_recache_datasets_with_RSEM
from glopenapi.db.mongo.index import update_metadata_value_lookup
from glopenapi.db.mongo.utils import iterate_mongo_connections
from glopenapi.db.mongo.types import ValueCheckedRecord
from glopenapi.db.mongo.utils import run_mongo_action, harmonize_document
from glopenapi.db.mongo.status import update_status
from glopenapi.common.utils import split_version as splver
from re import sub


class MetadataCacherLoop():
    """Lives in background and keeps local metadata cache, metadata index, and response cache up to date"""
    CLIENT_ATTRIBUTES_TO_COPY = (
        "adapter", "DatasetConstructor",
        "mongo_collections", "locale", "units_formatter",
    )
 
    def __init__(self, *, glopenapi_client, full_update_interval, full_update_retry_delay, dataset_init_interval, dataset_update_interval, min_app_version):
        """Prepare background loop that iteratively watches for changes to datasets"""
        self.glopenapi_client = glopenapi_client
        self._id = glopenapi_client.mongo_appname.replace(
            "GLOpenAPI", "MetadataCacherLoop",
        )
        for attr in self.CLIENT_ATTRIBUTES_TO_COPY:
            setattr(self, attr, getattr(glopenapi_client, attr))
        self.full_update_interval = full_update_interval
        self.full_update_retry_delay = full_update_retry_delay
        self.dataset_init_interval = dataset_init_interval
        self.dataset_update_interval = dataset_update_interval
        self.min_app_version = min_app_version
        self.status_kwargs = dict(collection=self.mongo_collections.status)
        super().__init__()
 
    def delay(self, timeout, desc=None):
        """Sleep for `timeout` seconds, reporting via GLOpenAPILogger.info()"""
        if desc:
            msg = f"Sleeping for {timeout} seconds before {desc}"
        else:
            msg = f"Sleeping for {timeout} seconds"
        GLOpenAPILogger.info(f"{self._id}:\n  {msg}")
        sleep(timeout)
 
    def __call__(self):
        """Continuously run MongoDB metadata cacher, inform caller of updated/failed/dropped accessions"""
        while True:
            ensure_indices(self.mongo_collections, self.locale)
            accessions, success = self.recache_metadata()
            if success:
                yield accessions
                self.delay(self.full_update_interval, "full metadata update")
            else:
                self.delay(
                    self.full_update_retry_delay,
                    "retrying connection for metadata update",
                )
 
    def get_accession_dispatcher(self):
        """Return dict of sets of accessions; populates: 'cached', 'live'; prepares empty: 'fresh', 'updated', 'stale', 'dropped', 'failed'"""
        GLOpenAPILogger.info(f"{self._id}:\n  Checking metadata cache")
        collection = self.mongo_collections.metadata
        return OrderedDict(
            cached=set(collection.distinct("id.accession")),
            live=set(self.adapter.get_accessions()), fresh=set(),
            updated=set(), stale=set(), dropped=set(), failed=set(),
        )
 
    def _drop_database_if_app_version_is_stale(self):
        """Destructive action: if app version logged in MongoDB is too old (or absent), drop the entire database"""
        mongo_client = self.glopenapi_client.mongo_client
        db_name = self.mongo_collections.metadata.database.name
        version_in_db = max((
            splver(e.get("version", "0")) for e in ({"version": "0"}, *(
                mongo_client[db_name].version_tracker.find({"info": "version"}))
            )
        ))
        if version_in_db < splver(self.min_app_version):
            GLOpenAPILogger.warning((lambda s: sub(r'\s+', " ", s).strip())(f"""
                DROPPING the MongoDB database {db_name!r} because the app
                version in the database ({version_in_db}) is older than the
                minimum target app version ({self.min_app_version})
            """))
            mongo_client.drop_database(db_name)
        mongo_client[db_name].version_tracker.delete_many({"info": "version"})
        mongo_client[db_name].version_tracker.insert_one({
            "info": "version", "version": self.glopenapi_client.app_version,
        })
 
    @apply_hack(precache_metadata_counts)
    @apply_hack(convert_legacy_metadata_pre)
    @apply_hack(TEMP_force_recache_datasets_with_RSEM)
    def recache_metadata(self):
        """Instantiate each available dataset; if contents changed, dataset automatically updates db.metadata"""
        self._drop_database_if_app_version_is_stale()
        try:
            accessions = self.get_accession_dispatcher()
        except Exception as exc:
            GLOpenAPILogger.error(f"{self._id}:\n  {exc!r}", exc_info=exc)
            return None, False
        def _iterate_with_delay():
            for acc in accessions["cached"] - accessions["live"]:
                yield (acc, *self.drop_single_dataset_metadata(acc))
            update_metadata_value_lookup(self.mongo_collections, self._id)
            for acc in accessions["live"] - accessions["cached"]:
                msg = f"retrieving metadata for new dataset {acc}"
                self.delay(self.dataset_init_interval, msg)
                yield (acc, *self.recache_single_dataset_metadata(acc, False))
            update_metadata_value_lookup(self.mongo_collections, self._id)
            for acc in accessions["live"] & accessions["cached"]:
                msg = f"updating cached metadata for dataset {acc}"
                self.delay(self.dataset_update_interval, msg)
                yield (acc, *self.recache_single_dataset_metadata(acc, True))
            update_metadata_value_lookup(self.mongo_collections, self._id)
        for acc, key, report, error in _iterate_with_delay():
            accessions[key].add(acc)
            _kws = dict(
                **self.status_kwargs, status=key, accession=acc,
                prefix=self._id, info=f"{acc} {report}", error=error,
            )
            update_status(**_kws)
            mongo_client = self.glopenapi_client.mongo_client
            n_apps = sum(1 for _ in iterate_mongo_connections(mongo_client))
            msg = f"Total number of active MongoDB connections: {n_apps}"
            GLOpenAPILogger.info(msg)
        GLOpenAPILogger.info(f"{self._id}, datasets:\n  " + ", ".join(
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
                except Exception as exc:
                    m = f"{self._id} @ {dataset.accession} samples:\n  {exc!r}"
                    GLOpenAPILogger.error(m, exc_info=exc)
                    return exc
                else:
                    return None
 
    @apply_hack(convert_legacy_metadata_post)
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
                dataset = self.DatasetConstructor(
                    accession=accession, files=files.value,
                    best_sample_name_matches=best_sample_name_matches,
                    status_kwargs=self.status_kwargs,
                )
            else:
                dataset = None
        except Exception as exc:
            msg = f"{self._id} @ {accession}:\n  {exc!r}"
            if has_cache:
                status = "stale"
                report = f"failed to retrieve ({exc!r}), kept stale"
                GLOpenAPILogger.warning(msg, exc_info=exc)
            else:
                status, report = "failed", f"failed to retrieve ({exc!r})"
                GLOpenAPILogger.error(msg, exc_info=exc)
            return status, report, exc
        if dataset is not None: # files have changed OR needs to be re-inserted
            self.drop_single_dataset_metadata(accession)
            exc = self.recache_single_dataset_samples(dataset)
            if exc is not None:
                self.drop_single_dataset_metadata(accession)
                return "failed", f"failed to parse ({exc!r})", exc
            else:
                return "updated", "updated", None
        else: # files have not changed
            return "fresh", "no action (fresh)", None
