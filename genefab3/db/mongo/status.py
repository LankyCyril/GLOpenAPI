from genefab3.db.mongo.utils import run_mongo_transaction
from datetime import datetime


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
    if (sample_name is None) and (status in {"failed", "dropped"}):
        run_mongo_transaction(
            action="delete_many", collection=collection,
            query=dict(accession=accession),
        )
    else:
        run_mongo_transaction(
            action="replace", collection=collection, query=query,
            data={"report timestamp": int(datetime.now().timestamp())},
        )
    if logger is not None:
        _lookup = dict(failed="error", dropped="error", warning="warning")
        log_kind = _lookup.get(status, "info")
        message = (
            "; ".join([str(_msg) for _msg in (info, warning, error) if _msg]) +
            " (" + repr(query) + ")"
        )
        getattr(logger, log_kind)(message)
