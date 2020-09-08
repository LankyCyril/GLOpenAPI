from genefab3.coldstorage.dataset import ColdStorageDataset
from genefab3.mongo.json import get_fresh_json
from genefab3.mongo.utils import replace_doc, harmonize_query
from datetime import datetime
from functools import partial


WARN_NO_META = "%s, %s: no metadata entries"
WARN_NO_STUDY = "%s, %s, %s: no Study entries"


class CachedDataset(ColdStorageDataset):
    """ColdStorageDataset via auto-updated metadata in database"""
 
    def __init__(self, db, accession, logger, init_assays=True):
        self.db, self.logger = db, logger
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
