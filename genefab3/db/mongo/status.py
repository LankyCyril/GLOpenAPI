from genefab3.db.mongo.utils import run_mongo_transaction
from datetime import datetime


def log_status(logger, status, info, warning, error, query):
    """Write log entry for status update"""
    _lookup = dict(failed="error", dropped="error", warning="warning")
    log_kind = _lookup.get(status, "info")
    getattr(logger, log_kind)(
        "; ".join([str(_msg) for _msg in (info, warning, error) if _msg]) +
        " (" + repr(query) + ")"
    )


def drop_status(collection, logger=None, accession=None, status=None, info=None, warning=None, error=None, **kwargs):
    """Drop all references to accession from `collection`"""
    query = {"accession": accession}
    run_mongo_transaction(
        action="delete_many", collection=collection, query=query,
    )
    if logger is not None:
        log_status(logger, status, info, warning, error, query)


def update_status(collection, logger=None, accession=None, assay_name=None, sample_name=None, status=None, info=None, warning=None, error=None, **kwargs):
    """Update status of dataset (and, optionally, assay/sample) in `collection`, log with logger"""
    query = {
        "status": status, "report type": (
            "dataset status" if sample_name is None else "parser message"
        ),
        "accession": accession, "assay name": assay_name,
        "sample name": sample_name,
        "info": info, "warning": warning,
        "error": None if (error is None) else type(error).__name__,
        "args": getattr(error, "args", []), "kwargs": kwargs,
    }
    run_mongo_transaction(
        action="replace", collection=collection, query=query,
        data={"report timestamp": int(datetime.now().timestamp())},
    )
    if logger is not None:
        log_status(logger, status, info, warning, error, query)
