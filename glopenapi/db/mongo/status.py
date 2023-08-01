from glopenapi.common.exceptions import GLOpenAPILogger
from glopenapi.db.mongo.utils import run_mongo_action
from datetime import datetime


def log_status(prefix, status, info, warning, error, query):
    """Write log entry for status update"""
    _lookup = dict(failed="error", stale="warning", warning="warning")
    getattr(GLOpenAPILogger, _lookup.get(status, "info"))(
        "; ".join([str(m) for m in (prefix, info, warning, error) if m]) +
        ":\n  " + repr({k: v for k, v in query.items() if v})
    )


def drop_status(collection, prefix="Status update", accession=None, status=None, info=None, warning=None, error=None, **kwargs):
    """Drop all references to accession from `collection`"""
    query = {"accession": accession}
    run_mongo_action(action="delete_many", collection=collection, query=query)
    log_status(prefix, status, info, warning, error, query)


def update_status(collection, prefix="Status update", report_type=None, accession=None, assay_name=None, sample_name=None, status=None, info=None, warning=None, error=None, **kwargs):
    """Update status of dataset (and, optionally, assay/sample) in `collection`, log with logger"""
    if status in {"dropped", "failed"}:
        drop_status(
            collection=collection, prefix=prefix, accession=accession,
            status=status, info=info, warning=warning, error=error, **kwargs,
        )
    query = {
        "status": status, "report type": report_type or (
            "dataset status" if sample_name is None else "parser message"
        ),
        "accession": accession, "assay name": assay_name,
        "sample name": sample_name, "info": info, "warning": warning,
        "error": None if (error is None) else type(error).__name__,
        "args": getattr(error, "args", []), "kwargs": kwargs,
    }
    run_mongo_action(
        action="replace", collection=collection, query=query,
        data={"report timestamp": int(datetime.now().timestamp())},
    )
    log_status(prefix, status, info, warning, error, query)
