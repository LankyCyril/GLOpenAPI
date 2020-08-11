from os import environ
from genefab3.config import DEBUG_MARKERS
from genefab3.mongo.meta import refresh_database_metadata
from genefab3.mongo.meta import get_dataset_with_caching
from genefab3.exceptions import GeneLabException


def debug(db, rargs={}):
    if environ.get("FLASK_ENV", None) not in DEBUG_MARKERS:
        return "Production server, debug disabled"
    else:
        all_accessions, fresh, stale, auf = refresh_database_metadata(db)
        return "<hr>".join([
            "All accessions:<br>" + ", ".join(sorted(all_accessions)),
            "Fresh accessions:<br>" + ", ".join(sorted(fresh)),
            "Stale accessions:<br>" + ", ".join(sorted(stale)),
            "Assays updated for:<br>" + ", ".join(sorted(auf)),
        ])


def get_assay_metadata(db, accession, assay_name, meta, context):
    glds = get_dataset_with_caching(db, accession)
    assay = glds.assays[assay_name]
    try:
        return getattr(assay, meta).full.reset_index()
    except AttributeError:
        raise GeneLabException("Unknown meta: '{}'".format(meta))
