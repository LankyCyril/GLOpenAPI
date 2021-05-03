from genefab3.common.utils import validate_no_backtick, random_string
from pandas import Index, MultiIndex, read_sql
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDatabaseException
from contextlib import contextmanager, closing, ExitStack
from sqlite3 import OperationalError, connect
from genefab3.common.logger import GeneFabLogger
from collections import OrderedDict
from genefab3.common.hacks import apply_hack, speedup_data_schema
from genefab3.api.renderers import Placeholders
from genefab3.common.types import DataDataFrame
from pandas.io.sql import DatabaseError as PandasDatabaseError
from genefab3.common.exceptions import GeneFabConfigurationException


class SQLiteIndexName(str): pass


class OndemandSQLiteDataFrame():
 
    @property
    def columns(self): return self._columns
    @columns.setter
    def columns(self, value):
        if isinstance(value, Index):
            all_levels, last_level = value, value.get_level_values(-1)
        else:
            all_levels, last_level = Index(value), value
        if set(last_level) <= set(self._raw_columns):
            self._raw_columns, self._columns = list(last_level), all_levels
        else:
            msg = f"Setting foreign column(s) to OndemandSQLiteDataFrame"
            foreign = sorted(set(last_level) - set(self._raw_columns))
            raise GeneFabFileException(msg, columns=foreign)
 
    @staticmethod
    def concat(objs, axis=1):
        """Concatenate OndemandSQLiteDataFrame objects without evaluation"""
        _t, _t_s = "OndemandSQLiteDataFrame", "OndemandSQLiteDataFrame_Single"
        _Single = OndemandSQLiteDataFrame_Single
        if len(objs) == 1:
            return objs[0]
        elif not all(isinstance(o, _Single) for o in objs):
            raise TypeError(f"{_t}.concat() on non-{_t_s} objects")
        elif axis != 1:
            raise ValueError(f"{_t}.concat(..., axis={axis}) makes no sense")
        elif len(set(obj.sqlite_db for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects from different database files"
            raise ValueError(msg)
        else:
            sqlite_db = objs[0].sqlite_db
            return OndemandSQLiteDataFrame_OuterJoined(sqlite_db, objs)


def _make_query_filter(table, where, limit, offset):
    """Validate arguments to OndemandSQLiteDataFrame_Single.get()"""
    if (offset != 0) and (limit is None):
        msg = "OndemandSQLiteDataFrame: `offset` without `limit`"
        raise GeneFabDatabaseException(msg, table=table)
    where_filter = "" if not where else f"WHERE {' AND '.join(where)}"
    limit_filter = "" if limit is None else f"LIMIT {limit} OFFSET {offset}"
    return f"{where_filter} {limit_filter}"


@contextmanager
def _make_view(connection, query):
    """Context manager temporarily creating an SQLite view from `query`"""
    viewname = random_string(seed=query)
    try:
        connection.cursor().execute(f"CREATE VIEW `{viewname}` as {query}")
    except OperationalError:
        msg = "Failed to create temporary view"
        _kw = dict(viewname=viewname, debug_info=query)
        raise GeneFabDatabaseException(msg, **_kw)
    else:
        query_repr = repr(query.lstrip()[:100] + "...")
        msg = f"Created temporary SQLite view {viewname} from {query_repr}"
        GeneFabLogger().info(msg)
        try:
            yield viewname
        finally:
            try:
                connection.cursor().execute(f"DROP VIEW `{viewname}`")
            except OperationalError:
                msg = f"Failed to drop temporary view {viewname}"
                GeneFabLogger().error(msg)
            else:
                msg = f"Dropped temporary SQLite view {viewname}"
                GeneFabLogger().info(msg)


class OndemandSQLiteDataFrame_Single(OndemandSQLiteDataFrame):
    """DataDataFrame to be retrieved from SQLite, possibly from multiple parts of same tabular file"""
 
    def __init__(self, sqlite_db, column_dispatcher):
        """Interpret `column_dispatcher`"""
        self.sqlite_db = sqlite_db
        self._column_dispatcher = column_dispatcher
        self.name = None
        self._raw_columns, _index_names = [], set()
        for n, p in column_dispatcher.items():
            validate_no_backtick(n, "column")
            validate_no_backtick(p, "table_part")
            if isinstance(n, SQLiteIndexName):
                _index_names.add(n)
            else:
                self._raw_columns.append(n)
            if self.name is None:
                self.name = p
        if len(_index_names) == 0:
            msg = "OndemandSQLiteDataFrame(): no index"
            raise GeneFabDatabaseException(msg, table=self.name)
        elif len(_index_names) > 1:
            msg = "OndemandSQLiteDataFrame(): parts indexes do not match"
            _kw = dict(table=self.name, index_names=_index_names)
            raise GeneFabDatabaseException(msg, **_kw)
        self.index = Index([], name=_index_names.pop())
        self._columns = Index(self._raw_columns, name=None)
 
    @property
    def _inverse_column_dispatcher(self):
        """Make dictionary {table_part -> [col, col, col, ...]}"""
        part_to_column = OrderedDict({
            # get index column from part containing first requested column:
            self._column_dispatcher[self._raw_columns[0]]: [self.index.name],
        })
        for column in self._raw_columns:
            part = self._column_dispatcher[column]
            part_to_column.setdefault(part, []).append(column)
        return part_to_column
 
    def __make_natural_join_query(self, part_to_column, where, limit=None, offset=0):
        """Generate SQL query for multipart NATURAL JOIN"""
        query_filter = _make_query_filter(self.name, where, limit, offset)
        joined = " NATURAL JOIN ".join(f"`{p}`" for p in part_to_column)
        first_table = next(iter(part_to_column))
        targets = ",".join((
            f"`{first_table}`.`{self.index.name}`",
            *(f"`{c}`" for c in self._raw_columns),
        ))
        _n, _m = len(self._raw_columns), len(part_to_column)
        _tt = "\n  ".join(("", *part_to_column))
        msg = f"retrieving {_n} columns from {_m} table(s):{_tt}"
        GeneFabLogger().info(f"{self.name}; {msg}")
        return f"SELECT {targets} FROM {joined} {query_filter}"
 
    @apply_hack(speedup_data_schema)
    def get(self, *, where=None, limit=None, offset=0, context=None):
        """Interpret arguments and retrieve data as DataDataFrame by running SQL queries"""
        part_to_column = self._inverse_column_dispatcher
        if len(part_to_column) == 0:
            return Placeholders.EmptyDataDataFrame()
        else:
            args = part_to_column, where, limit, offset
            query = self.__make_natural_join_query(*args)
            with closing(connect(self.sqlite_db)) as connection:
                try:
                    a, k = (query, connection), dict(index_col=self.index.name)
                    data = read_sql(*a, **k)
                except (OperationalError, PandasDatabaseError) as e:
                    if "too many columns" in str(e).lower():
                        msg = "Too many columns requested"
                        sug = "Limit request to fewer than 2000 columns"
                        _kw = dict(table=self.name, suggestion=sug)
                        raise GeneFabDatabaseException(msg, **_kw)
                    else:
                        msg = "No data found"
                        raise GeneFabDatabaseException(msg, table=self.name)
                else:
                    msg = f"retrieved from SQLite as pandas DataFrame"
                    GeneFabLogger().info(f"{self.name}; {msg}")
                    data.columns = self.columns
                    return DataDataFrame(data)
 
    @contextmanager
    def view(self, connection):
        """Interpret arguments and temporarily expose requested data as SQL view"""
        part_to_column = self._inverse_column_dispatcher
        if len(part_to_column) == 0:
            yield None
        else:
            args = part_to_column, None, None, 0
            join_query = self.__make_natural_join_query(*args)
            try:
                with _make_view(connection, join_query) as viewname:
                    yield viewname, self._raw_columns
            except (OperationalError, GeneFabDatabaseException):
                raise GeneFabDatabaseException("No data found", table=self.name)


class OndemandSQLiteDataFrame_OuterJoined(OndemandSQLiteDataFrame):
    """DataDataFrame to be retrieved from SQLite, full outer joined from multiple views"""
 
    def __init__(self, sqlite_db, objs):
        _t = "OndemandSQLiteDataFrame"
        self.sqlite_db, self.objs = sqlite_db, objs
        if len(objs) < 2:
            raise ValueError(f"{type(self).__name__}: no objects to join")
        elif len(set(obj.index.name for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing index names"
            raise ValueError(msg)
        else:
            self.index = Index([], name=objs[0].index.name)
        if len(set(obj.columns.nlevels for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing column `nlevels`"
            raise ValueError(msg)
        else:
            if objs[0].columns.nlevels == 1:
                _mkindex = Index
            else:
                _mkindex = MultiIndex.from_tuples
            self._columns = _mkindex(
                sum((obj.columns.to_list() for obj in objs), []),
            )
            self._raw_columns = self._columns.get_level_values(-1)
            names = ", ".join(str(obj.name) for obj in objs)
            self.name = f"FullOuterJoin({names})"
 
    @contextmanager
    def view(self, connection):
        """Interpret arguments and retrieve data as DataDataFrame by running SQL queries"""
        with ExitStack() as stack:
            object_views = [
                stack.enter_context(o.view(connection)) for o in self.objs
            ]
            left_view, left_columns = object_views[0]
            for right_view, right_columns in object_views[1:]:
                merged_columns = left_columns + right_columns
                targets = ",".join((
                    f"`{self.index.name}`", *(f"`{c}`" for c in merged_columns),
                ))
                condition = f"""`{left_view}`.`{self.index.name}` ==
                    `{right_view}`.`{self.index.name}`"""
                query = f"""SELECT `{left_view}`.{targets} FROM
                        `{left_view}` LEFT JOIN `{right_view}` ON {condition}
                    UNION SELECT `{right_view}`.{targets} FROM
                        `{right_view}` LEFT JOIN `{left_view}` ON {condition}
                        WHERE `{left_view}`.`{self.index.name}` IS NULL"""
                merged_view = stack.enter_context(_make_view(connection, query))
                left_view, left_columns = merged_view, merged_columns
            yield merged_view, merged_columns
 
    @apply_hack(speedup_data_schema)
    def get(self, *, where=None, limit=None, offset=0, context=None):
        """Interpret arguments and retrieve data as DataDataFrame by running SQL queries"""
        query_filter = _make_query_filter(self.name, where, limit, offset)
        with closing(connect(self.sqlite_db)) as connection:
            with self.view(connection) as (merged_view, merged_columns):
                try:
                    q = f"SELECT * FROM `{merged_view}` {query_filter}"
                    data = read_sql(q, connection, index_col=self.index.name)
                except OperationalError:
                    msg = "No data found"
                    raise GeneFabDatabaseException(msg, table=self.name)
                except PandasDatabaseError as e:
                    msg = "Bad SQL query when joining tables"
                    _kw = dict(table=self.name, debug_info=[repr(e), q])
                    raise GeneFabConfigurationException(msg, **_kw)
                else:
                    msg = f"retrieved from SQLite as pandas DataFrame"
                    GeneFabLogger().info(f"{self.name}; {msg}")
                    data.columns = self.columns
                    return DataDataFrame(data)
