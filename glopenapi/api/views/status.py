from os import path
from datetime import datetime
from glopenapi.db.mongo.utils import iterate_mongo_connections
from glopenapi.common.utils import iterate_terminal_leaves
from glopenapi.common.exceptions import GLOpenAPIParserException
from glopenapi.api.renderers.types import StreamedAnnotationTable
from itertools import chain


GiB = 1024**3
ID_COLUMNS = "accession", "assay name", "sample name"
INFO_COLUMNS = "report timestamp", "report type", "status"
MESSAGE_COLUMNS = "error", "warning"
ATTR_COLUMNS = "args", "kwargs"


def sqlite_db_report(db_name, descriptor):
    return {"information": {
        "report type": f"size of {db_name}, GiB",
        "status": (
            format(path.getsize(descriptor["db"]) / GiB, ".3f")
            if (descriptor["db"] and path.isfile(descriptor["db"])) else "0"
        ),
        "report timestamp": int(datetime.now().timestamp()),
    }}


def mongo_db_report(mongo_client):
    return {"information": {
        "report type": "number of active MongoDB connections",
        "status": sum(1 for _ in iterate_mongo_connections(mongo_client)),
        "report timestamp": int(datetime.now().timestamp()),
    }}


def get(*, glopenapi_client, sqlite_dbs, context):
    for _ in iterate_terminal_leaves(context.query):
        msg = "Metadata queries are not valid for view"
        raise GLOpenAPIParserException(msg, view="status")
    table = StreamedAnnotationTable(
        cursor=chain(
            [sqlite_db_report(n, d) for n, d in sqlite_dbs.__dict__.items()],
            [mongo_db_report(glopenapi_client.mongo_client)],
            glopenapi_client.mongo_collections.status.aggregate([
                {"$group": {"_id": {
                    "id": {c: f"${c}" for c in ID_COLUMNS},
                    "information": {c: f"${c}" for c in INFO_COLUMNS},
                    "messages": {c: f"${c}" for c in MESSAGE_COLUMNS},
                    "report attributes": {c: f"${c}" for c in ATTR_COLUMNS},
                }}},
                {"$replaceRoot": {"newRoot": "$_id"}},
            ]),
        ),
        na_rep=None,
    )
    table.cacheable = False
    return table