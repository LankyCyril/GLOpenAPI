from os import path
from datetime import datetime
from genefab3.db.mongo.utils import iterate_mongo_connections
from genefab3.common.utils import iterate_terminal_leaves
from genefab3.common.exceptions import GeneFabParserException
from genefab3.common.types import StreamedAnnotationTable
from itertools import chain


GiB = 1024**3
ID_COLUMNS = ("accession", "assay name", "sample name",)
REPORT_COLUMNS = (
    "report type", "status", "warning", "error", "args", "kwargs",
    "report timestamp",
)


def sqlite_db_report(db_name, descriptor):
    return {"report": {
        "report type": f"size of {db_name}, GiB",
        "status": (
            format(path.getsize(descriptor["db"]) / GiB, ".3f")
            if (descriptor["db"] and path.isfile(descriptor["db"])) else "0"
        ),
        "report timestamp": int(datetime.now().timestamp()),
    }}


def mongo_db_report(mongo_client):
    return {"report": {
        "report type": f"number of active MongoDB connections",
        "status": sum(1 for _ in iterate_mongo_connections(mongo_client)),
        "report timestamp": int(datetime.now().timestamp()),
    }}


def get(*, genefab3_client, sqlite_dbs, context):
    for _ in iterate_terminal_leaves(context.query):
        msg = "Metadata queries are not valid for view"
        raise GeneFabParserException(msg, view="status")
    return StreamedAnnotationTable(
        cursor=chain(
            [sqlite_db_report(n, d) for n, d in sqlite_dbs.__dict__.items()],
            genefab3_client.mongo_collections.status.aggregate([
                {"$group": {"_id": {
                    "id": {c: f"${c}" for c in ID_COLUMNS},
                    "report": {c: f"${c}" for c in REPORT_COLUMNS},
                }}},
                {"$replaceRoot": {"newRoot": "$_id"}},
            ]),
        ),
    )
