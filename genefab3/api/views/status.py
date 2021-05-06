from pandas import MultiIndex, DataFrame, concat
from os import path
from numpy import nan
from datetime import datetime
from genefab3.db.mongo.utils import iterate_mongo_connections
from genefab3.common.utils import iterate_terminal_leaves, blackjack_normalize
from genefab3.common.exceptions import GeneFabParserException
from itertools import chain, cycle


def INPLACE_set_id_as_index(dataframe):
    """Move all columns with first level value of "id" into MultiIndex"""
    if "id" in dataframe.columns.get_level_values(0):
        dataframe.index = MultiIndex.from_frame(
            dataframe["id"], names=dataframe[["id"]].columns,
        )
        dataframe.drop(columns="id", inplace=True)


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
            if (descriptor["db"] and path.isfile(descriptor["db"])) else nan
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
    database_status = DataFrame((
        [sqlite_db_report(n, d) for n, d in sqlite_dbs.__dict__.items()] +
        [mongo_db_report(genefab3_client.mongo_client)]
    ))
    datasets_status = blackjack_normalize(
        genefab3_client.mongo_collections.status.find(
            {}, {"_id": False, **{c: True for c in STATUS_COLUMNS}},
        ),
        max_level=0,
    )
    astype_args = datasets_status[["args", "kwargs"]].astype(str)
    astype_args[astype_args.isin({"[]", "{}", "()"})] = nan
    datasets_status[["args", "kwargs"]] = astype_args
    status_df = concat([database_status, datasets_status])
    _iso = lambda t: datetime.utcfromtimestamp(t).isoformat() + "Z"
    status_df["report timestamp"] = status_df["report timestamp"].apply(_iso)
    status_df = status_df[[c for c in STATUS_COLUMNS if c in status_df]]
    status_df.columns = MultiIndex.from_tuples(
        zip(chain(["id"]*3, cycle(["report"])), status_df.columns),
    )
    INPLACE_set_id_as_index(status_df)
    return status_df.sort_values(
        by=[("report", "report timestamp"), ("report", "report type")],
        ascending=[False, True],
    )
