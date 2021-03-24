from genefab3.db.mongo.utils import run_mongo_transaction
from datetime import datetime


def update_status(collection, logger=None, accession=None, assay_name=None, sample_name=None, status=None, info=None, warning=None, error=None, **kwargs):
    """Update status of dataset (and, optionally, assay/sample) in `collection`, log with logger"""
    if sample_name is None:
        if status in {"failed", "dropped"}:
            run_mongo_transaction(
                action="delete_many", collection=collection,
                query=dict(accession=accession),
            )
    replacement_query = {
        "kind": "dataset" if sample_name is None else "sample",
        "accession": accession, "sample name": sample_name,
        "warning": warning,
    }
    inserted_data = {
        "status": status, "info": info, "warning": warning,
        "error": None if (error is None) else type(error).__name__,
        "report timestamp": int(datetime.now().timestamp()),
        "assay name": assay_name,
        "args": getattr(error, "args", []), "kwargs": kwargs,
    }
    run_mongo_transaction(
        action="replace", collection=collection,
        query=replacement_query, data=inserted_data,
    )
    if logger is not None:
        _lookup = dict(failed="error", dropped="error", warning="warning")
        log_kind = _lookup.get(status, "info")
        message = (
            "; ".join([str(_msg) for _msg in (info, warning, error) if _msg]) +
            " (" + repr(replacement_query) + ")"
        )
        getattr(logger, log_kind)(message)
