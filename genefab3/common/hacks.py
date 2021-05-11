from functools import wraps
from genefab3.common.utils import random_unique_string
from genefab3.db.sql.utils import sql_connection
from sqlite3 import OperationalError
from pandas.io.sql import DatabaseError as PandasDatabaseError
from numpy import nan
from pandas import DataFrame
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.types import StreamedDataTable, NaN
from genefab3.common.exceptions import GeneFabFormatException
from genefab3.common.logger import GeneFabLogger
from genefab3.common.exceptions import GeneFabConfigurationException
from re import search, sub, IGNORECASE


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
        sub_data, found = {}, lambda v: v is not None
        for t, m, M, h in zip(targets, minima, maxima, hasnan):
            _min = m if found(m) else M if found(M) else nan
            _max = M if found(M) else _min
            _nan = nan if h else _max
            sub_data[t.strip("`")] = [_min, _max, _nan]
        sub_source = "SCHEMA_HACK:" + random_unique_string()
        try:
            DataFrame(sub_data).to_sql(
                sub_source, connection, if_exists="replace",
            )
        except (OperationalError, PandasDatabaseError) as e:
            msg = "Schema speedup failed: could create substitute table"
            raise GeneFabDatabaseException(msg, debug_info=repr(e))
        sub_query = sub(
            r'(select\s+.*\s+from\s+)[^\s]*$', fr'\1`{sub_source}`',
            table.query.rstrip(), flags=IGNORECASE,
        )
        if sub_query == table.query.rstrip():
            execute(f"DROP TABLE IF EXISTS `{sub_source}`")
            raise GeneFabConfigurationException(
                "Schema speedup failed: could not substitute source table",
                debug_info=table.query.rstrip(),
            )
        else:
            return sub_source, sub_query


def speed_up_data_schema(get, self, *, context, limit=None, offset=0):
    """If context.schema == '1', replaces underlying query with quick retrieval of just values informative for schema"""
    from genefab3.db.sql.streamed_tables import StreamedDataTableWizard
    if context.schema != "1":
        return get(self, context=context, limit=limit, offset=offset)
    elif context.data_comparisons or context.data_columns or limit or offset:
        msg = "Table manipulation is not supported when requesting schema"
        sug = "Remove comparisons and/or column, row slicing from query"
        raise GeneFabFormatException(msg, suggestion=sug)
    elif isinstance(self, StreamedDataTableWizard):
        msg = f"apply_hack(speed_up_data_schema) for {self.name}"
        GeneFabLogger().info(msg)
        table = get(self, context=context, limit=limit, offset=offset)
        match = search(r'select\s+(.*)\s+from\s', table.query, flags=IGNORECASE)
        if match:
            targets = match.group(1).split(",")
            sub_source, sub_query = _make_sub(table, targets)
        else:
            msg = "Schema speedup failed: could not infer target columns"
            raise GeneFabConfigurationException(msg, debug_info=table.query)
        table.__del__()
        return StreamedDataTable(
            sqlite_db=self.sqlite_db, query=sub_query,
            source=sub_source, na_rep=NaN,
        )
    else:
        msg = "Schema speedup applied to unsupported object type"
        raise GeneFabConfigurationException(msg, type=type(self))
