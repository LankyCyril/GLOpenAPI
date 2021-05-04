from contextlib import contextmanager, closing, ExitStack
from genefab3.common.utils import random_unique_string, validate_no_backtick
from sqlite3 import OperationalError, connect
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.logger import GeneFabLogger
from pandas import Index, MultiIndex, read_sql
from genefab3.common.exceptions import GeneFabFileException
from functools import lru_cache, partial
from numpy import isreal
from re import search, sub
from genefab3.common.hacks import apply_hack, speed_up_data_schema
from pandas.io.sql import DatabaseError as PandasDatabaseError
from genefab3.common.types import DataDataFrame
from collections import OrderedDict


@contextmanager
def mkselect(connection, query, kind="TABLE"):
    """Context manager temporarily creating an SQLite view or table from `query`"""
    selectname = random_unique_string(seed=query)
    try:
        connection.cursor().execute(f"CREATE {kind} `{selectname}` as {query}")
    except OperationalError:
        msg = f"Failed to create temporary {kind}"
        _kw = dict(name=selectname, debug_info=query)
        raise GeneFabDatabaseException(msg, **_kw)
    else:
        query_repr = repr(query.lstrip()[:100] + "...")
        msg = f"Created temporary SQLite {kind} {selectname} from {query_repr}"
        GeneFabLogger().info(msg)
        try:
            yield selectname
        finally:
            try:
                connection.cursor().execute(f"DROP {kind} `{selectname}`")
            except OperationalError:
                msg = f"Failed to drop temporary {kind} {selectname}"
                GeneFabLogger().error(msg)
            else:
                msg = f"Dropped temporary SQLite {kind} {selectname}"
                GeneFabLogger().info(msg)


class SQLiteIndexName(str): pass


class OndemandSQLiteDataFrame():
 
    @property
    def columns(self):
        return self._columns
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
 
    @property
    def _columns_raw2slashed(self):
        return {c[-1]: "/".join(c) for c in self.columns}
 
    @lru_cache(maxsize=16384)
    def _column_as_slashed(self, table, column):
        return f"`{table}`.`{column}` as `{self._columns_raw2slashed[column]}`"
 
    @property
    def _columns_slashed2full(self):
        return {"/".join(c): c for c in self.columns}
 
    @property
    def _columns_raw2full(self):
        return {c[-1]: c for c in self.columns}
 
    @property
    def _columns_raw_name_counts(self):
        return self.columns.get_level_values(-1).value_counts()
 
    def _column_passed2full(self, passed_name, ignore_missing=False):
        """Match passed column name to full column name found in self.columns"""
        full_name = self._columns_slashed2full.get(
            passed_name, (
                self._columns_raw2full.get(passed_name)
                if self._columns_raw_name_counts.get(passed_name) == 1
                else self._columns_raw_name_counts.get(passed_name, 0)
            ),
        )
        if isreal(full_name):
            if full_name == 0:
                if ignore_missing:
                    return None
                else:
                    msg = "Requested column not in table"
                    raise GeneFabFileException(msg, column=passed_name)
            else:
                msg = "Ambiguous column requested"
                sug = "Use full syntax (columns.ACCESSION/ASSAY/COLUMN)"
                _kw = dict(column=passed_name, suggestion=sug)
                raise GeneFabFileException(msg, **_kw)
        else:
            return full_name
 
    def constrain_columns(self, context):
        """Constrain self.columns to specified columns, if any"""
        if context.data_columns:
            self.columns = MultiIndex.from_tuples([
                self._column_passed2full(c) for c in context.data_columns
            ])
 
    def _sanitize_where(self, context):
        """Infer column names for SQLite WHERE as columns are presented in table or view"""
        passed2full = getattr(
            self, "_unique_column_passed2full", self._column_passed2full,
        )
        for dc in getattr(context, "data_comparisons", []):
            match = search(r'(`)([^`]*)(`)', dc)
            if not match:
                msg = "Not a valid column in data comparison"
                raise GeneFabFileException(msg, comparison=dc)
            else:
                sanitized_name = "/".join(passed2full(match.group(2)))
                yield sub(r'(`)([^`]*)(`)', f"`{sanitized_name}`", dc, count=1)
 
    def _make_query_filter(self, context, limit, offset):
        """Validate arguments to OndemandSQLiteDataFrame_Single.get()"""
        where = list(self._sanitize_where(context))
        if (offset != 0) and (limit is None):
            msg = "OndemandSQLiteDataFrame: `offset` without `limit`"
            raise GeneFabDatabaseException(msg, table=self.name)
        where_filter = "" if not where else f"WHERE {' AND '.join(where)}"
        limit_filter = "" if limit is None else f"LIMIT {limit} OFFSET {offset}"
        return f"{where_filter} {limit_filter}"
 
    @apply_hack(speed_up_data_schema)
    def get(self, *, context, limit=None, offset=0):
        """Interpret arguments and retrieve data as DataDataFrame by running SQL queries"""
        query_filter = self._make_query_filter(context, limit, offset)
        final_targets = ",".join((
            f"`{self.index.name}`", *(f"`{'/'.join(c)}`" for c in self.columns),
        ))
        with closing(connect(self.sqlite_db)) as connection:
            with self.select(connection, kind="VIEW") as (select, _):
                try:
                    q = f"SELECT {final_targets} FROM `{select}` {query_filter}"
                    data = read_sql(q, connection, index_col=self.index.name)
                except (OperationalError, PandasDatabaseError) as e:
                    if "too many columns" in str(e).lower():
                        msg = "Too many columns requested"
                        sug = "Limit request to fewer than 2000 columns"
                        _kw = dict(table=self.name, suggestion=sug)
                        raise GeneFabDatabaseException(msg, **_kw)
                    else:
                        msg = "Data could not be retrieved"
                        _kw = dict(table=self.name, debug_info=[repr(e), q])
                        raise GeneFabDatabaseException(msg, **_kw)
                else:
                    msg = f"retrieved from SQLite as pandas DataFrame"
                    GeneFabLogger().info(f"{self.name}; {msg}")
                    data.columns = self.columns
                    return DataDataFrame(data)
 
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
        icd = OrderedDict({
            # get index column from part containing first requested column:
            self._column_dispatcher[self._raw_columns[0]]: [self.index.name],
        })
        for column in self._raw_columns:
            icd.setdefault(self._column_dispatcher[column], []).append(column)
        return icd
 
    @contextmanager
    def select(self, connection, kind="TABLE"):
        """Temporarily expose requested data as SQL view or table for OndemandSQLiteDataFrame_OuterJoined.select()"""
        _n, _icd = len(self._raw_columns), self._inverse_column_dispatcher
        _tt = "\n  ".join(("", *_icd))
        msg = f"retrieving {_n} columns from {len(_icd)} table(s):{_tt}"
        GeneFabLogger().info(f"{self.name}; {msg}")
        join_statement = " NATURAL JOIN ".join(f"`{p}`" for p in _icd)
        _as = partial(self._column_as_slashed, table=self.name)
        columns_as_slashed_columns = [_as(column=c) for c in self._raw_columns]
        query = f"""
            SELECT `{self.index.name}`,{','.join(columns_as_slashed_columns)}
            FROM {join_statement}"""
        try:
            with mkselect(connection, query, kind=kind) as selectname:
                yield selectname, [
                    f"`{self._columns_raw2slashed[c]}`"
                    for c in self._raw_columns
                ]
        except (OperationalError, GeneFabDatabaseException):
            msg = "Data could not be retrieved"
            raise GeneFabDatabaseException(msg, table=self.name)


class OndemandSQLiteDataFrame_OuterJoined(OndemandSQLiteDataFrame):
    """DataDataFrame to be retrieved from SQLite, full outer joined from multiple views or tables"""
 
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
 
    def _unique_column_passed2full(self, passed_name):
        """Match passed column name to unique full column name found in self.objs[*].columns"""
        matches_and_misses = {
            o._column_passed2full(passed_name, ignore_missing=True)
            for o in self.objs
        }
        matches = matches_and_misses - {None}
        if not matches:
            msg = "Requested column not in table"
            raise GeneFabFileException(msg, column=passed_name)
        elif len(matches) > 1:
            msg = "Ambiguous column requested"
            sug = "Use full syntax (columns.ACCESSION/ASSAY/COLUMN)"
            _kw = dict(column=passed_name, suggestion=sug)
            raise GeneFabFileException(msg, **_kw)
        else:
            return matches.pop()
 
    @contextmanager
    def select(self, connection, kind="TABLE"):
        """Temporarily expose requested data as SQL view or table"""
        with ExitStack() as stack:
            enter_context = stack.enter_context
            selects = [enter_context(o.select(connection)) for o in self.objs]
            agg_select, agg_columns = selects[0]
            for i, (next_select, next_columns) in enumerate(selects[1:], 1):
                agg_columns = agg_columns + next_columns
                agg_targets = ",".join(agg_columns)
                condition = f"""`{agg_select}`.`{self.index.name}` ==
                    `{next_select}`.`{self.index.name}`"""
                query = f"""
                    SELECT `{agg_select}`.`{self.index.name}`,{agg_targets}
                        FROM `{agg_select}` LEFT JOIN `{next_select}`
                            ON {condition}
                    UNION
                    SELECT `{next_select}`.`{self.index.name}`,{agg_targets}
                        FROM `{next_select}` LEFT JOIN `{agg_select}`
                            ON {condition}
                            WHERE `{agg_select}`.`{self.index.name}` IS NULL"""
                if (i == len(selects) - 1) and (kind == "VIEW"):
                    _kind = "VIEW"
                else:
                    _kind = "TABLE"
                agg_select = enter_context(mkselect(connection, query, _kind))
            yield agg_select, None
