from functools import wraps, reduce, partial
from genefab3.db.sql.utils import SQLTransaction
from genefab3.common.types import NaN, StreamedDataTable
from pandas import DataFrame
from sqlite3 import OperationalError
from genefab3.common.exceptions import GeneFabDatabaseException, GeneFabLogger
from genefab3.common.exceptions import GeneFabConfigurationException
from re import search
from pandas import merge


def apply_hack(hack):
    """Wraps `method` with function `hack`"""
    def outer(method):
        @wraps(method)
        def inner(*args, **kwargs):
            return hack(method, *args, **kwargs)
        return inner
    return outer


def get_sub_df(obj, partname, partcols):
    """Retrieve only informative values from single part of table as pandas.DataFrame"""
    from genefab3.db.sql.streamed_tables import SQLiteIndexName
    found = lambda v: v is not None
    index_name, data = None, {}
    desc = "hacks/get_sub_df"
    with SQLTransaction(obj.sqlite_db, desc) as (connection, execute):
        try:
            fetch = lambda query: execute(query).fetchone()
            mktargets = lambda f: ",".join(f"{f}(`{c}`)" for c in partcols)
            mkquery = lambda t: f"SELECT {t} FROM `{partname}` LIMIT 1"
            minima = fetch(mkquery(mktargets("MIN")))
            maxima = fetch(mkquery(mktargets("MAX")))
            counts = fetch(mkquery(mktargets("COUNT")))
            n_rows = fetch(f"SELECT COUNT(*) FROM `{partname}` LIMIT 1")[0]
            hasnan = [(n_rows - c) > 0 for c in counts]
            for c, m, M, h in zip(partcols, minima, maxima, hasnan):
                _min = m if found(m) else M if found(M) else NaN
                _max = M if found(M) else _min
                _nan = NaN if h else _max
                if isinstance(c, SQLiteIndexName):
                    index_name = str(c)
                data[c] = [_min, _max, _nan]
        except OperationalError as e:
            msg = "Data could not be retrieved"
            _kw = dict(table=obj.name, debug_info=repr(e))
            raise GeneFabDatabaseException(msg, **_kw)
    dataframe = DataFrame(data)
    if index_name is not None:
        dataframe.set_index(index_name, inplace=True)
    return dataframe


class StreamedDataTableSub(StreamedDataTable):
    """StreamedDataTable-like class that streams from underlying pandas.DataFrame"""
 
    def __init__(self, sub_merged, sub_columns, na_rep=None):
        self.shape, self.n_index_levels = tuple(sub_merged.shape), 1
        self._dataframe = sub_merged
        self._index_name = sub_merged.index.name
        self._columns = sub_columns
        self.na_rep = na_rep
        self.accessions = {c[0] for c in self._columns}
        self.datatypes, self.gct_validity_set = set(), set()
 
    @property
    def index(self):
        """Iterate index line by line, like in pandas"""
        if self.n_index_levels:
            if self.na_rep is None:
                for value in self._dataframe.index:
                    yield (value,)
            else:
                _na_tup = (self.na_rep,)
                for value in self._dataframe.index:
                    yield _na_tup if value is None else (value,)
        else:
            yield from ([] for _ in range(self.shape[0]))
 
    @property
    def values(self):
        """Iterate values line by line, like in pandas"""
        if self.na_rep is None:
            if self.n_index_levels:
                for _, vv in self._dataframe.iterrows():
                    yield vv.tolist()
            else:
                for r, vv in self._dataframe.iterrows():
                    yield (r, *vv)
        else:
            if self.n_index_levels:
                for _, vv in self._dataframe.iterrows():
                    yield [self.na_rep if v is None else v for v in vv.tolist()]
            else:
                for r, vv in self._dataframe.iterrows():
                    yield [self.na_rep if v is None else v for v in (r, *vv)]


def speed_up_data_schema(get, self, *, context, limit=None, offset=0):
    """If context.schema == '1', replaces underlying query with quick retrieval of just values informative for schema"""
    if context.schema != "1":
        return get(self, context=context, limit=limit, offset=offset)
    elif context.data_comparisons:
        msg = "Using column comparisons when requesting schema"
        raise NotImplementedError(msg)
    else:
        from genefab3.db.sql.streamed_tables import (
            SQLiteIndexName,
            StreamedDataTableWizard_Single, StreamedDataTableWizard_OuterJoined,
        )
        GeneFabLogger(info=f"apply_hack(speed_up_data_schema) for {self.name}")
        sub_dfs, sub_columns, index_name = [], [], []
        def _extend_parts(obj):
            for partname, partcols in obj._inverse_column_dispatcher.items():
                if isinstance(partcols[0], SQLiteIndexName):
                    index_name.clear()
                    index_name.append(partcols[0])
                    sub_df = get_sub_df(obj, partname, partcols)
                else:
                    sub_df = get_sub_df(obj, partname, [*index_name, *partcols])
                print(sub_df.T)
                sub_dfs.append(sub_df)
                _ocr2f = obj._columns_raw2full
                sub_columns.extend(_ocr2f[c] for c in sub_df.columns)
        if isinstance(self, StreamedDataTableWizard_Single):
            _extend_parts(self)
        elif isinstance(self, StreamedDataTableWizard_OuterJoined):
            for obj in self.objs:
                _extend_parts(obj)
        else:
            msg = "Schema speedup applied to unsupported object type"
            raise GeneFabConfigurationException(msg, type=type(self))
        _kw = dict(left_index=True, right_index=True, how="outer", sort=False)
        sub_merged = reduce(partial(merge, **_kw), sub_dfs)
        print(sub_merged.T)
        return StreamedDataTableSub(sub_merged, sub_columns)


def bypass_uncached_views(get, self, context):
    """If serving favicon, bypass checking response_cache"""
    if (context.view == "") or search(r'^favicon\.[A-Za-z0-9]+$', context.view):
        from genefab3.common.types import ResponseContainer
        return ResponseContainer(content=None)
    else:
        return get(self, context)
