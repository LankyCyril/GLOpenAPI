from argparse import Namespace
from genefab3.coldstorage.dataset import ColdStorageDataset
from genefab3.mongo.json import get_fresh_json
from genefab3.mongo.utils import replace_doc, harmonize_query
from datetime import datetime
from functools import partial
from genefab3.exceptions import GeneLabDatabaseException
from re import split
from genefab3.utils import UniversalSet


WARN_NO_META = "%s, %s: no metadata entries"
WARN_NO_STUDY = "%s, %s, %s: no Study entries"


def NoLogger():
    """Placeholder that masquerades as a logger but does not do anything"""
    return Namespace(warning=lambda *args, **kwargs: None)


class CachedDataset(ColdStorageDataset):
    """ColdStorageDataset via auto-updated metadata in database"""
 
    def __init__(self, db, accession, logger=None, init_assays=True):
        self.db = db
        self.logger = logger if (logger is not None) else NoLogger()
        super().__init__(
            accession, init_assays=False,
            get_json=partial(get_fresh_json, db=db),
        )
        try:
            if init_assays:
                self.init_assays()
                if any(self.changed.__dict__.values()):
                    for assay_name, assay in self.assays.items():
                        db.metadata.delete_many({
                            ".accession": accession, ".assay": assay_name,
                        })
                        if assay.meta:
                            db.metadata.insert_many(
                                harmonize_query(assay.meta.values()),
                            )
                            for sample_name in assay.meta:
                                if "Study" not in assay.meta[sample_name]:
                                    logger.warning(
                                        WARN_NO_STUDY, accession,
                                        assay_name, sample_name,
                                    )
                        else:
                            logger.warning(WARN_NO_META, accession, assay_name)
            else:
                self.assays = CachedAssayDispatcher(self)
            replace_doc(
                db.dataset_timestamps, {"accession": accession},
                {"last_refreshed": int(datetime.now().timestamp())},
                harmonize=True,
            )
        except:
            self.drop_cache()
            raise
 
    def drop_cache(self=None, db=None, accession=None):
        (db or self.db).dataset_timestamps.delete_many({
            "accession": accession or self.accession,
        })
        (db or self.db).metadata.delete_many({
            ".accession": accession or self.accession,
        })
        (db or self.db).json_cache.delete_many({
            "identifier": accession or self.accession,
        })


class CachedAssayDispatcher(dict):
    """Lazily exposes a dataset's assays sourced from MongoDB metadata, indexable by name"""
    def __init__(self, dataset):
        self.dataset = dataset
    def __getitem__(self, assay_name):
        return CachedAssay(self.dataset, assay_name)


class CachedAssay():
    """Exposes individual assay information and metadata"""
 
    def __init__(self, dataset, assay_name):
        self.dataset = dataset
        self.name = assay_name
        self.db = dataset.db
 
    def get_file_descriptors(self, name=None, regex=None, glob=None, projection=None):
        """Given mask and/or target field, find filenames, urls, and datestamps"""
        if projection:
            metadata_candidates = set()
            query = {".accession": self.dataset.accession, ".assay": self.name}
            full_projection = {"_id": False, **projection}
            for entry in self.db.metadata.find(query, full_projection):
                while isinstance(entry, dict):
                    if len(entry) == 1:
                        entry = next(iter(entry.values()))
                    elif len(entry) > 1:
                        raise GeneLabDatabaseException(
                            "Single-field lookup encountered multiple children",
                        )
                if isinstance(entry, str):
                    metadata_candidates.update(split(r'\s*,\s*', entry))
        else:
            metadata_candidates = None
        if sum(arg is not None for arg in (name, regex, glob)) > 0:
            fileinfo = self.dataset.get_file_descriptors(
                name=name, regex=regex, glob=glob,
            )
            return {
                filename: descriptor
                for filename, descriptor in fileinfo.items()
                if filename in (metadata_candidates or UniversalSet())
            }
        elif metadata_candidates:
            return {
                filename: descriptor for fileinfo in (
                    self.dataset.get_file_descriptors(name=name)
                    for name in metadata_candidates
                )
                for filename, descriptor in fileinfo.items()
            }
        else:
            return {} # not enough information to perform lookups
