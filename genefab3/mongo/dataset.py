from genefab3.coldstorage.dataset import ColdStorageDataset
from genefab3.mongo.json import get_fresh_json
from genefab3.mongo.utils import replace_doc, harmonize_query as nize
from datetime import datetime
from functools import partial


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
                        for collection in db.metadata, db.annotations:
                            collection.delete_many({
                                ".accession": accession, ".assay": assay_name,
                            })
                        if assay.metadata:
                            db.metadata.insert_many(nize(assay.metadata))
                        else:
                            msg_mask = "%s, %s: no metadata entries"
                            logger.warning(msg_mask, accession, assay_name)
                        if assay.annotation:
                            db.annotations.insert_many(nize(assay.annotation))
                        else:
                            msg_mask = "%s, %s: no annotation entries"
                            logger.warning(msg_mask, accession, assay_name)
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
        (db or self.db).annotations.delete_many({
            ".accession": accession or self.accession,
        })
        (db or self.db).json_cache.delete_many({
            "identifier": accession or self.accession,
        })
