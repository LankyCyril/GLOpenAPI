from pandas import json_normalize, concat
from datetime import datetime
from numpy import nan


STATUS_COLUMNS = [
    "report timestamp", "report type", "accession", "assay name",
    "sample name", "status", "warning", "error", "args", "kwargs",
]


def get(mongo_collections):
    status_json = mongo_collections.status.find(
        {}, {"_id": False, **{c: True for c in STATUS_COLUMNS}},
    )
    status_df = json_normalize(list(status_json), max_level=0).sort_values(
        by="report timestamp", ascending=False,
    )
    status_df = status_df[[c for c in STATUS_COLUMNS if c in status_df]]
    status_df["report timestamp"] = status_df["report timestamp"].apply(
        lambda t: datetime.utcfromtimestamp(t).isoformat() + "Z"
    )
    return concat(
        {"database status": status_df.applymap(lambda v: v or nan)},
        axis=1,
    )