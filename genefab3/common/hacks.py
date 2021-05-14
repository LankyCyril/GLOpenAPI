from functools import wraps
from genefab3.db.sql.utils import SQLTransaction
from genefab3.common.types import NaN, StreamedDataTable
from genefab3.common.utils import random_unique_string
from pandas import DataFrame
from sqlite3 import OperationalError
from pandas.io.sql import DatabaseError as PandasDatabaseError
from genefab3.common.exceptions import GeneFabDatabaseException, GeneFabLogger
from genefab3.common.exceptions import GeneFabConfigurationException
from re import search


def apply_hack(hack):
    """Wraps `method` with function `hack`"""
    def outer(method):
        @wraps(method)
        def inner(*args, **kwargs):
            return hack(method, *args, **kwargs)
        return inner
    return outer


class _TempSchemaSource():
    """Temporary source for hacked table; similar to TempSelect()"""
    def __init__(self, sqlite_db):
        self.sqlite_db = sqlite_db
        self.name = "SCHEMA_HACK:" + random_unique_string()
    def __del__(self):
        desc = "tables/hacks/_TempSchemaSource/__del__"
        with SQLTransaction(self.sqlite_db, desc) as (_, execute):
            try:
                execute(f"DROP TABLE `{self.name}`")
            except OperationalError as e:
                msg = f"Failed to drop temporary SQLite TABLE {self.name}"
                GeneFabLogger(error=msg, exc_info=e)
            else:
                msg = f"Dropped temporary SQLite TABLE {self.name}"
                GeneFabLogger(info=msg)


def _make_sub(self, table):
    """Make substitute source and query for quick retrieval of values informative for schema"""
    sql_targets = [self._index_name, *("/".join(c) for c in self.columns)]
    functargets = lambda f: ",".join(f"{f}(`{st}`)" for st in sql_targets)
    source_name, query_filter = table.source_select.name, table.query_filter
    mkquery = lambda t: f"SELECT {t} FROM `{source_name}` {query_filter}"
    n_rows_query = f"SELECT COUNT(*) FROM `{source_name}` {query_filter}"
    desc = "tables/hacks/_make_sub"
    with SQLTransaction(table.sqlite_db, desc) as (connection, execute):
        fetch = lambda query: execute(query).fetchone()
        minima = fetch(mkquery(functargets("MIN")))
        maxima = fetch(mkquery(functargets("MAX")))
        counts = fetch(mkquery(functargets("COUNT")))
        n_rows = fetch(n_rows_query)[0]
        hasnan = [(n_rows - c) > 0 for c in counts]
        sub_source = _TempSchemaSource(sqlite_db=table.sqlite_db)
        sub_data = DataFrame(columns=sql_targets)
        for t, m, M, h in zip(sql_targets, minima, maxima, hasnan):
            _min = m if (m is not None) else (M if (M is not None) else NaN)
            _max = M if (M is not None) else _min
            _nan = NaN if h else _max
            sub_data[t] = [_min, _max, _nan]
        try:
            sub_data.set_index(self._index_name, inplace=True)
            sub_data.to_sql(sub_source.name, connection, if_exists="replace")
        except (OperationalError, PandasDatabaseError) as e:
            msg = "Schema speedup failed: could not create substitute table"
            raise GeneFabDatabaseException(msg, debug_info=repr(e))
        else:
            return sub_source


def speed_up_data_schema(get, self, *, context, limit=None, offset=0):
    """If context.schema == '1', replaces underlying query with quick retrieval of just values informative for schema"""
    from genefab3.db.sql.streamed_tables import StreamedDataTableWizard
    table = get(self, context=context, limit=limit, offset=offset)
    if context.schema != "1":
        return table
    elif isinstance(self, StreamedDataTableWizard):
        GeneFabLogger(info=f"apply_hack(speed_up_data_schema) for {self.name}")
        return StreamedDataTable(
            sqlite_db=self.sqlite_db, source_select=_make_sub(self, table),
            targets=table.targets, query_filter=table.query_filter, na_rep=NaN,
        )
    else:
        msg = "Schema speedup applied to unsupported object type"
        raise GeneFabConfigurationException(msg, type=type(self))


def bypass_uncached_views(get, self, context):
    """If serving favicon, bypass checking response_cache"""
    if (context.view == "") or search(r'^favicon\.[A-Za-z0-9]+$', context.view):
        from genefab3.common.types import ResponseContainer
        return ResponseContainer(content=None)
    else:
        return get(self, context)
