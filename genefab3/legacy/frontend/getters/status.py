from genefab3.config import COLLECTION_NAMES
from pandas import json_normalize, concat
from datetime import datetime


STATUS_COLUMNS = [
    "report timestamp", "kind", "status", "accession", "assay name",
    "warning", "error", "details",
]


def get_status(mongo_db, context, cname=COLLECTION_NAMES.STATUS):
    """Retrieve reports from db.status"""
    status_json = getattr(mongo_db, cname).find(
        {}, {"_id": False, **{c: True for c in STATUS_COLUMNS}},
    )
    status_df = json_normalize(list(status_json))[STATUS_COLUMNS]
    status_df["report timestamp"] = status_df["report timestamp"].apply(
        lambda t: datetime.utcfromtimestamp(t).isoformat() + "Z"
    )
    return concat({"database status": status_df}, axis=1)
