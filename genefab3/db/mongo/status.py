from genefab3.db.mongo.utils import run_mongo_transaction
from datetime import datetime
from genefab3.common.logger import GeneFabLogger, log_to_mongo_collection


def update_status(status_collection, log_collection=None, accession=None, assay_name=None, sample_name=None, status=None, info=None, warning=None, error=None, **kwargs):
    """Update status of dataset (and, optionally, assay/sample) in `status_collection`, log with logger and to `log_collection`"""
    if sample_name is None:
        replacement_query = {"kind": "dataset", "accession": accession}
        if status in {"failed", "dropped"}:
            run_mongo_transaction(
                action="delete_many", collection=status_collection,
                query={"accession": accession},
            )
    else:
        replacement_query = {
            "kind": "sample",
            "accession": accession, "sample name": sample_name,
        }
    inserted_data = {
        "status": status, "info": info, "warning": warning,
        "error": None if (error is None) else type(error).__name__,
        "report timestamp": int(datetime.now().timestamp()),
        "assay name": assay_name, "details": [],
    }
    if error is not None:
        inserted_data["details"].extend(getattr(error, "args", []))
    run_mongo_transaction(
        action="replace", collection=status_collection,
        query=replacement_query, data=inserted_data,
    )
    _lookup = dict(failed="error", dropped="error", warning="warning")
    log_kind = _lookup.get(status, "info")
    message = (
        "; ".join([str(_msg) for _msg in (info, warning, error) if _msg]) +
        " (" + repr(replacement_query) + ")"
    )
    getattr(GeneFabLogger(), log_kind)(message)
    if log_collection:
        log_to_mongo_collection(
            log_collection, is_exception=False, et=log_kind.upper(), ev=message,
        )
