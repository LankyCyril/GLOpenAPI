from genefab3.mongo.meta import get_dataset_with_caching
from genefab3.exceptions import GeneLabException


def get_assay_metadata(db, accession, assay_name, meta, context):
    glds = get_dataset_with_caching(db, accession)
    assay = glds.assays[assay_name]
    try:
        return getattr(assay, meta).full.reset_index()
    except AttributeError:
        raise GeneLabException("Unknown meta: '{}'".format(meta))
