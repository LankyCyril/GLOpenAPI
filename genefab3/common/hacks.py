from functools import wraps
from genefab3.common.exceptions import GeneFabConfigurationException


def apply_hack(hack):
    def outer(method):
        @wraps(method)
        def inner(*args, **kwargs):
            return hack(method, *args, **kwargs)
        return inner
    return outer


def get_OSDF_Single_schema(self, *, where=None, limit=None, offset=0, context=None):
    raise ValueError


def get_OSDF_OuterJoined_schema(self, *, where=None, limit=None, offset=0, context=None):
    raise ValueError


def speedup_data_schema(get, self, *, where=None, limit=None, offset=0, context=None):
    kwargs = dict(where=where, limit=limit, offset=offset, context=context)
    if context.schema != "1":
        return get(self, **kwargs)
    else:
        from genefab3.db.sql.pandas import OndemandSQLiteDataFrame_Single
        from genefab3.db.sql.pandas import OndemandSQLiteDataFrame_OuterJoined
        if isinstance(self, OndemandSQLiteDataFrame_Single):
            return get_OSDF_Single_schema(self, **kwargs)
        elif isinstance(self, OndemandSQLiteDataFrame_OuterJoined):
            return get_OSDF_OuterJoined_schema(self, **kwargs)
        else:
            msg = "Schema speedup applied to unsupported object type"
            raise GeneFabConfigurationException(msg, type=type(self))
