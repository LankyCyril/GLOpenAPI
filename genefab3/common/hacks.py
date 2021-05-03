from functools import wraps, reduce, partial
from contextlib import closing
from sqlite3 import connect, OperationalError
from numpy import nan
from pandas import DataFrame, merge
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.types import DataDataFrame
from genefab3.common.exceptions import GeneFabFormatException
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


def get_OSDF_Single_schema(self):
    """Replaces OndemandSQLiteDataFrame_Single.get() with retrieval of just values informative for 'schema=1'"""
    from genefab3.db.sql.pandas import SQLiteIndexName
    found = lambda v: v is not None
    index_name, data = None, {}
    with closing(connect(self.sqlite_db)) as connection:
        try:
            fetch = lambda query: connection.cursor().execute(query).fetchone()
            whitelist = {self.index.name, *self.columns.get_level_values(-1)}
            for part, columns in self._inverse_column_dispatcher.items():
                mktargets = lambda f: ",".join(f"{f}(`{c}`)" for c in columns)
                mkquery = lambda t: f"SELECT {t} FROM `{part}` LIMIT 1"
                minima = fetch(mkquery(mktargets("MIN")))
                maxima = fetch(mkquery(mktargets("MAX")))
                counts = fetch(mkquery(mktargets("COUNT")))
                n_rows = fetch(f"SELECT COUNT(*) FROM `{part}` LIMIT 1")[0]
                hasnan = [(n_rows - c) > 0 for c in counts]
                for c, m, M, h in zip(columns, minima, maxima, hasnan):
                    _min = m if found(m) else M if found(M) else nan
                    _max = M if found(M) else _min
                    _nan = nan if h else _max
                    if isinstance(c, SQLiteIndexName):
                        index_name = str(c)
                    if c in whitelist:
                        data[c] = [_min, _max, _nan]
        except OperationalError:
            raise GeneFabDatabaseException("No data found", table=self.name)
        else:
            dataframe = DataFrame(data)
            if (set(dataframe.columns) != whitelist):
                msg = "Failed to apply schema speedup, columns did not match"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                if index_name is not None:
                    dataframe.set_index(index_name, inplace=True)
                dataframe = dataframe[self.columns.get_level_values(-1)]
                dataframe.columns = self.columns
            return DataDataFrame(dataframe)


def get_OSDF_OuterJoined_schema(self, *, context):
    """Replaces OndemandSQLiteDataFrame_OuterJoined.get() with retrieval of just values informative for 'schema=1'"""
    merge_kws = dict(left_index=True, right_index=True, how="outer", sort=False)
    return DataDataFrame(reduce(
        partial(merge, **merge_kws),
        (obj.get(context=context) for obj in self.objs),
    ))


def speed_up_data_schema(get, self, *, context, limit=None, offset=0):
    """If context.schema == '1', replaces OndemandSQLiteDataFrame.get() with quick retrieval of just values informative schema"""
    if context.schema != "1":
        kwargs = dict(context=context, limit=limit, offset=offset)
        return get(self, **kwargs)
    elif context.data_comparisons or context.data_columns or limit or offset:
        msg = "Table manipulation is not supported when requesting schema"
        sug = "Remove comparisons and/or column, row slicing from query"
        raise GeneFabFormatException(msg, suggestion=sug)
    else:
        from genefab3.db.sql.pandas import OndemandSQLiteDataFrame_Single
        from genefab3.db.sql.pandas import OndemandSQLiteDataFrame_OuterJoined
        msg = f"apply_hack(speed_up_data_schema) for {self.name}"
        GeneFabLogger().info(msg)
        if isinstance(self, OndemandSQLiteDataFrame_Single):
            return get_OSDF_Single_schema(self)
        elif isinstance(self, OndemandSQLiteDataFrame_OuterJoined):
            return get_OSDF_OuterJoined_schema(self, context=context)
        else:
            msg = "Schema speedup applied to unsupported object type"
            raise GeneFabConfigurationException(msg, type=type(self))
