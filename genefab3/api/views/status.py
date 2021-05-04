from os import path
from numpy import nan
from datetime import datetime
from genefab3.db.mongo.utils import iterate_mongo_connections
from genefab3.common.utils import iterate_terminal_leaves, blackjack_normalize
from genefab3.common.exceptions import GeneFabParserException
from pandas import MultiIndex
from itertools import chain, cycle
from genefab3.api.views.metadata import INPLACE_set_id_as_index


GiB = 1024**3

STATUS_COLUMNS = [
    "accession", "assay name", "sample name", "report type", "status",
    "warning", "error", "args", "kwargs", "report timestamp",
]


def sqlite_db_report(db_name, descriptor):
    return {
        "report type": f"size of {db_name}, GiB",
        "status": (
            format(path.getsize(descriptor["db"]) / GiB, ".3f")
            if descriptor["db"] else nan
        ),
        "report timestamp": int(datetime.now().timestamp()),
    }


def mongo_db_report(mongo_client):
    return {
        "report type": f"number of active MongoDB connections",
        "status": sum(1 for _ in iterate_mongo_connections(mongo_client)),
        "report timestamp": int(datetime.now().timestamp()),
    }


def get(*, genefab3_client, sqlite_dbs, context):
    for _ in iterate_terminal_leaves(context.query):
        msg = "Metadata queries are not valid for view"
        raise GeneFabParserException(msg, view="status")
    else:
        status_json = genefab3_client.mongo_collections.status.find(
            {}, {"_id": False, **{c: True for c in STATUS_COLUMNS}},
        )
    status_df = blackjack_normalize(list(status_json), max_level=0).sort_values(
        by=["report timestamp", "report type"], ascending=[False, True],
    )
    status_df = status_df[[c for c in STATUS_COLUMNS if c in status_df]]
    _iso = lambda t: datetime.utcfromtimestamp(t).isoformat() + "Z"
    astype_args = status_df[["args", "kwargs"]].astype(str)
    astype_args[astype_args.isin({"[]", "{}", "()"})] = nan
    status_df[["args", "kwargs"]] = astype_args
    status_df = status_df.append( # TODO order
        other=(
            [sqlite_db_report(n, d) for n, d in sqlite_dbs.__dict__.items()] +
            [mongo_db_report(genefab3_client.mongo_client)]
        ),
        ignore_index=True,
    )
    status_df["report timestamp"] = status_df["report timestamp"].apply(_iso)
    status_df.columns = MultiIndex.from_tuples(
        zip(chain(["id"]*3, cycle(["report"])), status_df.columns),
    )
    INPLACE_set_id_as_index(status_df)
    return status_df
