from functools import wraps
from genefab3.db.sql.utils import sql_connection
from genefab3.common.types import NaN, StreamedDataTable
from genefab3.common.utils import random_unique_string
from pandas import DataFrame
from sqlite3 import OperationalError
from re import sub, search, IGNORECASE
from pandas.io.sql import DatabaseError as PandasDatabaseError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.logger import GeneFabLogger
from genefab3.common.exceptions import GeneFabConfigurationException


def apply_hack(hack):
    """Wraps `method` with function `hack`"""
    def outer(method):
        @wraps(method)
        def inner(*args, **kwargs):
            return hack(method, *args, **kwargs)
        return inner
    return outer


def _make_sub(table, targets):
    """Make substitute source and query for quick retrieval of values informative for schema"""
    with sql_connection(table.sqlite_db, "tables") as (connection, execute):
        functargets = lambda f: ",".join(f"{f}({t})" for t in targets)
        mkquery = lambda t: f"SELECT {t} FROM `{table.source}` LIMIT 1"
        fetch = lambda query: execute(query).fetchone()
        minima = fetch(mkquery(functargets("MIN")))
        maxima = fetch(mkquery(functargets("MAX")))
        counts = fetch(mkquery(functargets("COUNT")))
        n_rows = fetch(f"SELECT COUNT(*) FROM `{table.source}` LIMIT 1")[0]
        hasnan = [(n_rows - c) > 0 for c in counts]
        sub_data, sub_source = {}, "SCHEMA_HACK:" + random_unique_string()
        for t, m, M, h in zip(targets, minima, maxima, hasnan):
            _min = m if (m is not None) else M if (M is not None) else NaN
            _max = M if (M is not None) else _min
            _nan = NaN if h else _max
            sub_data[t.strip("`")] = [_min, _max, _nan]
        try:
            _kw = dict(if_exists="replace")
            DataFrame(sub_data).to_sql(sub_source, connection, **_kw)
        except (OperationalError, PandasDatabaseError) as e:
            table.__del__() # actually will probably clean up on exit anyway...
            msg = "Schema speedup failed: could not create substitute table"
            raise GeneFabDatabaseException(msg, debug_info=repr(e))
        sub_query = sub(
            r'(select\s+.*\s+from\s+)[^\s]*$', fr'\1`{sub_source}`',
            table.query.rstrip(), flags=IGNORECASE,
        )
        if sub_query == table.query.rstrip():
            table.__del__() # actually will probably clean up on exit anyway...
            execute(f"DROP TABLE IF EXISTS `{sub_source}`")
            msg = "Schema speedup failed: could not substitute table in query"
            raise GeneFabConfigurationException(msg, debug_info=table.query)
        else:
            return sub_source, sub_query


def speed_up_data_schema(get, self, *, context, limit=None, offset=0):
    """If context.schema == '1', replaces underlying query with quick retrieval of just values informative for schema"""
    from genefab3.db.sql.streamed_tables import StreamedDataTableWizard
    table = get(self, context=context, limit=limit, offset=offset)
    if context.schema != "1":
        return table
    elif isinstance(self, StreamedDataTableWizard):
        msg = f"apply_hack(speed_up_data_schema) for {self.name}"
        GeneFabLogger().info(msg)
        match = search(r'select\s+(.*)\s+from\s', table.query, flags=IGNORECASE)
        if match:
            targets = match.group(1).split(",")
            sub_source, sub_query = _make_sub(table, targets)
        else:
            table.__del__() # actually will probably clean up on exit anyway...
            msg = "Schema speedup failed: could not infer target columns"
            raise GeneFabConfigurationException(msg, debug_info=table.query)
        return StreamedDataTable(
            sqlite_db=self.sqlite_db, query=sub_query,
            source=sub_source, na_rep=NaN,
        )
    else:
        msg = "Schema speedup applied to unsupported object type"
        raise GeneFabConfigurationException(msg, type=type(self))
