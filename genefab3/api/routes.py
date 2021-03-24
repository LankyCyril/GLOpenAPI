from functools import wraps, partial
from genefab3.common.exceptions import GeneFabException
from flask import Response


class Routes():
 
    def __init__(self, mongo_collections):
        self.mongo_collections = mongo_collections
 
    def _as_endpoint(method, endpoint=None):
        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)
        if endpoint:
            wrapper.endpoint = endpoint
        elif hasattr(method, "__name__") and isinstance(method.__name__, str):
            wrapper.endpoint = "/" + method.__name__ + "/"
        return wrapper
 
    def items(self):
        for name in dir(self):
            method = getattr(self, name)
            if isinstance(getattr(method, "endpoint", None), str):
                yield method.endpoint, method
 
    @partial(_as_endpoint, endpoint="/favicon.<imgtype>")
    def favicon(self, imgtype):
        return ""
 
    @partial(_as_endpoint, endpoint="/debug/error/")
    def debug_error(self):
        raise GeneFabException("Generic error test")
        return "OK (raised exception)"
 
    @partial(_as_endpoint, endpoint="/")
    def root(self):
        return "Hello space"
 
    @_as_endpoint
    def status(self):
        """Simple placeholder status report, will be superseded with renderable dataframe"""
        from pandas import json_normalize, concat
        from datetime import datetime
        from numpy import nan
        STATUS_COLUMNS = [
            "report timestamp", "kind", "accession", "assay name",
            "sample name", "status", "warning", "error", "args", "kwargs",
        ]
        status_json = self.mongo_collections.status.find(
            {}, {"_id": False, **{c: True for c in STATUS_COLUMNS}},
        )
        status_df = json_normalize(list(status_json), max_level=0).sort_values(
            by=["kind", "report timestamp"], ascending=[True, False],
        )
        status_df = status_df[[c for c in STATUS_COLUMNS if c in status_df]]
        status_df["report timestamp"] = status_df["report timestamp"].apply(
            lambda t: datetime.utcfromtimestamp(t).isoformat() + "Z"
        )
        status_twolevel_df = concat(
            {"database status": status_df.applymap(lambda v: v or nan)},
            axis=1,
        )
        return Response(
            "<style>table {table-layout: fixed; white-space: nowrap}</style>" +
            status_twolevel_df.fillna("").to_html(index=False, col_space="1in"),
            mimetype="text/html",
        )
