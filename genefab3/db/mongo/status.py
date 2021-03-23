from genefab3.db.mongo.utils import run_mongo_transaction
from datetime import datetime


def update_status(collection, accession, sample_name=None, status="success", info=None, warning=None, error=None, details=(), **kwargs):
    """Update status of dataset (and, optionally, assay/sample) in `collection`"""
    if sample_name is None:
        replacement_query = {"kind": "dataset", "accession": accession}
        if status == "failure":
            run_mongo_transaction(
                action="delete_many", collection=collection,
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
        "details": list(details),
        "report timestamp": int(datetime.now().timestamp()),
    }
    if error is not None:
        inserted_data["details"].extend(getattr(error, "args", []))
    run_mongo_transaction(
        action="replace", collection=collection,
        query=replacement_query, data=inserted_data,
    )
