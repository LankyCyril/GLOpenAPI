from argparse import Namespace
from genefab3.coldstorage.dataset import ColdStorageDataset
from genefab3.mongo.json import get_fresh_json
from genefab3.mongo.utils import replace_document, harmonize_document
from datetime import datetime
from functools import partial
from genefab3.mongo.assay import CachedAssay


WARN_NO_META = "%s, %s: no metadata entries"
WARN_NO_STUDY = "%s, %s, %s: no Study entries"


def NoLogger():
    """Placeholder that masquerades as a logger but does not do anything"""
    return Namespace(warning=lambda *args, **kwargs: None)


class CachedDataset(ColdStorageDataset):
    """ColdStorageDataset via auto-updated metadata in database"""
 
    def __init__(self, db, accession, logger=None, init_assays=True, metadata_units_format=None):
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
                            db.metadata.insert_many(harmonize_document(
                                assay.meta.values(),
                                units_format=metadata_units_format,
                            ))
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
            replace_document(
                db.dataset_timestamps, {"accession": accession},
                {"last_refreshed": int(datetime.now().timestamp())},
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
