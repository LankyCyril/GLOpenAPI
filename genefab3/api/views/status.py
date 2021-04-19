from pandas import json_normalize, MultiIndex
from datetime import datetime
from numpy import nan
from itertools import chain, cycle


STATUS_COLUMNS = [
    "accession", "assay name", "sample name", "report type", "status",
    "warning", "error", "args", "kwargs", "report timestamp",
]


def get(mongo_collections):
    status_json = mongo_collections.status.find(
        {}, {"_id": False, **{c: True for c in STATUS_COLUMNS}},
    )
    status_df = json_normalize(list(status_json), max_level=0).sort_values(
        by=["report timestamp", "report type"], ascending=[False, True],
    )
    status_df = status_df[[c for c in STATUS_COLUMNS if c in status_df]]
    status_df["report timestamp"] = status_df["report timestamp"].apply(
        lambda t: datetime.utcfromtimestamp(t).isoformat() + "Z"
    )
    astype_args = status_df[["args", "kwargs"]].astype(str)
    astype_args[astype_args.isin({"[]", "{}", "()"})] = nan
    status_df[["args", "kwargs"]] = astype_args
    status_df.columns = MultiIndex.from_tuples(
        zip(chain(["id"]*3, cycle(["report"])), status_df.columns),
    )
    return status_df
