from contextlib import contextmanager, ExitStack
from genefab3.common.utils import random_unique_string, validate_no_backtick
from sqlite3 import OperationalError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.logger import GeneFabLogger
from genefab3.common.types import StreamedDataTable
from genefab3.common.exceptions import GeneFabFileException
from collections import Counter, OrderedDict
from numpy import isreal
from re import search, sub
from genefab3.common.hacks import apply_hack, speed_up_data_schema
from genefab3.db.sql.utils import sql_connection


@contextmanager
def mkselect(execute, query, kind="TABLE", keep=False):
    """Context manager temporarily creating an SQLite view or table from `query`"""
    selectname = "TEMP:" + random_unique_string(seed=query)
    try:
        execute(f"CREATE {kind} `{selectname}` as {query}")
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
            if keep:
                msg = f"KEEPING temporary SQLite {kind} {selectname}"
                GeneFabLogger().info(msg)
            else:
                try:
                    execute(f"DROP {kind} `{selectname}`")
                except OperationalError:
                    msg = f"Failed to drop temporary {kind} {selectname}"
                    GeneFabLogger().error(msg)
                else:
                    msg = f"Dropped temporary SQLite {kind} {selectname}"
                    GeneFabLogger().info(msg)


class SQLiteIndexName(str): pass


class StreamedDataTableWizard():
 
    @property
    def columns(self):
        return self._columns
    @columns.setter
    def columns(self, passed_columns):
        passed_last_level = [c[-1] for c in passed_columns]
        own_last_level = [c[-1] for c in self._columns]
        if set(passed_last_level) <= set(own_last_level):
            self._columns = passed_columns
        else:
            msg = f"Setting foreign column(s) to StreamedDataTableWizard"
            foreign = sorted(set(passed_last_level) - set(own_last_level))
            raise GeneFabFileException(msg, columns=foreign)
 
    @property
    def _columns_raw2slashed(self):
        return {c[-1]: "/".join(c) for c in self.columns}
 
    @property
    def _columns_slashed2full(self):
        return {"/".join(c): c for c in self.columns}
 
    @property
    def _columns_raw2full(self):
        return {c[-1]: c for c in self.columns}
 
    def _column_passed2full(self, passed_name, ignore_missing=False):
        """Match passed column name to full column name found in self.columns"""
        _raw_name_counts = Counter(c[-1] for c in self.columns)
        full_name = self._columns_slashed2full.get(
            passed_name, (
                self._columns_raw2full.get(passed_name)
                if _raw_name_counts.get(passed_name) == 1
                else _raw_name_counts.get(passed_name, 0)
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
            self.columns = [
                self._column_passed2full(c) for c in context.data_columns
            ]
 
    def _sanitize_where(self, context):
        """Infer column names for SQLite WHERE as columns are presented in table or view"""
        passed2full = getattr(
            self,
            # defined in StreamedDataTableWizard_OuterJoined:
            "_unique_column_passed2full",
            # defined in StreamedDataTableWizard/StreamedDataTableWizard_Single:
            self._column_passed2full,
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
        """Validate arguments for StreamedDataTableWizard.get() and append WHERE, LIMIT, OFFSET clauses"""
        where = list(self._sanitize_where(context))
        if (offset != 0) and (limit is None):
            msg = "StreamedDataTableWizard: `offset` without `limit`"
            raise GeneFabDatabaseException(msg, table=self.name)
        where_filter = "" if not where else f"WHERE {' AND '.join(where)}"
        limit_filter = "" if limit is None else f"LIMIT {limit} OFFSET {offset}"
        return f"{where_filter} {limit_filter}"
 
    @apply_hack(speed_up_data_schema)
    def get(self, *, context, limit=None, offset=0):
        """Interpret arguments and retrieve data as StreamedDataTable by running SQL queries"""
        query_filter = self._make_query_filter(context, limit, offset)
        final_targets = ",".join((
            f"`{self._index_name}`",
            *(f"`{'/'.join(c)}`" for c in self.columns),
        ))
        with sql_connection(self.sqlite_db, "tables") as (_, execute):
            with self.select(execute, kind="TABLE", keep=True) as (select, _):
                msg = f"passing SQLite table {select} to StreamedDataTable"
                GeneFabLogger().info(f"{self.name}; {msg}")
        data = StreamedDataTable(
            sqlite_db=self.sqlite_db,
            query=f"SELECT {final_targets} FROM `{select}` {query_filter}",
            index_col=self._index_name, override_columns=self.columns,
            source=select,
        )
        msg = f"retrieving from SQLite as StreamedDataTable"
        GeneFabLogger().info(f"{self.name}; {msg}")
        return data
 
    @staticmethod
    def concat(objs, axis=1):
        """Concatenate StreamedDataTableWizard objects without evaluation"""
        _t, _t_s = "StreamedDataTableWizard", "StreamedDataTableWizard_Single"
        _Single = StreamedDataTableWizard_Single
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
            return StreamedDataTableWizard_OuterJoined(sqlite_db, objs)


class StreamedDataTableWizard_Single(StreamedDataTableWizard):
    """StreamedDataTable to be retrieved from SQLite, possibly from multiple parts of same tabular file"""
 
    def __init__(self, sqlite_db, column_dispatcher):
        """Interpret `column_dispatcher`"""
        self.sqlite_db = sqlite_db
        self._column_dispatcher = column_dispatcher
        self.name = None
        self._columns, _index_names = [], set()
        for n, p in column_dispatcher.items():
            validate_no_backtick(n, "column")
            validate_no_backtick(p, "table_part")
            if isinstance(n, SQLiteIndexName):
                _index_names.add(n)
            else:
                self._columns.append([n])
            if self.name is None:
                self.name = p
        if len(_index_names) == 0:
            msg = "StreamedDataTableWizard(): no index"
            raise GeneFabDatabaseException(msg, table=self.name)
        elif len(_index_names) > 1:
            msg = "StreamedDataTableWizard(): indexes of parts do not match"
            _kw = dict(table=self.name, index_names=_index_names)
            raise GeneFabDatabaseException(msg, **_kw)
        self._index_name = _index_names.pop()
 
    @property
    def _inverse_column_dispatcher(self):
        """Make dictionary {table_part -> [col, col, col, ...]}"""
        icd = OrderedDict({
            # get index column from part containing first requested column:
            self._column_dispatcher[self.columns[0][-1]]: [self._index_name],
        })
        for *_, rawcol in self.columns:
            icd.setdefault(self._column_dispatcher[rawcol], []).append(rawcol)
        return icd
 
    @contextmanager
    def select(self, execute, kind="TABLE", keep=False):
        """Temporarily expose requested data as SQL view or table for StreamedDataTableWizard_OuterJoined.select()"""
        _n, _icd = len(self.columns), self._inverse_column_dispatcher
        _tt = "\n  ".join(("", *_icd))
        msg = f"retrieving {_n} columns from {len(_icd)} table(s):{_tt}"
        GeneFabLogger().info(f"{self.name}; {msg}")
        join_statement = " NATURAL JOIN ".join(f"`{p}`" for p in _icd)
        columns_as_slashed_columns = [
            f"""`{self._column_dispatcher[rawcol]}`.`{rawcol}`
                as `{self._columns_raw2slashed[rawcol]}`"""
            for *_, rawcol in self.columns
        ]
        query = f"""
            SELECT `{self._index_name}`,{','.join(columns_as_slashed_columns)}
            FROM {join_statement}"""
        try:
            with mkselect(execute, query, kind=kind, keep=keep) as selectname:
                yield selectname, [
                    f"`{self._columns_raw2slashed[rawcol]}`"
                    for *_, rawcol in self.columns
                ]
        except (OperationalError, GeneFabDatabaseException):
            msg = "Data could not be retrieved"
            raise GeneFabDatabaseException(msg, table=self.name)


class StreamedDataTableWizard_OuterJoined(StreamedDataTableWizard):
    """StreamedDataTable to be retrieved from SQLite, full outer joined from multiple views or tables"""
 
    def __init__(self, sqlite_db, objs):
        _t = "StreamedDataTableWizard"
        self.sqlite_db, self.objs = sqlite_db, objs
        if len(objs) < 2:
            raise ValueError(f"{type(self).__name__}: no objects to join")
        elif len(set(obj._index_name for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing index names"
            raise ValueError(msg)
        else:
            self._index_name = objs[0]._index_name
            self._columns = sum((obj.columns for obj in objs), [])
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
    def select(self, execute, kind="TABLE", keep=False):
        """Temporarily expose requested data as SQL view or table"""
        with ExitStack() as stack:
            enter_context = stack.enter_context
            selects = [enter_context(o.select(execute)) for o in self.objs]
            agg_select, agg_columns = selects[0]
            for i, (next_select, next_columns) in enumerate(selects[1:], 1):
                agg_columns = agg_columns + next_columns
                agg_targets = ",".join(agg_columns)
                condition = f"""`{agg_select}`.`{self._index_name}` ==
                    `{next_select}`.`{self._index_name}`"""
                query = f"""
                    SELECT `{agg_select}`.`{self._index_name}`,{agg_targets}
                        FROM `{agg_select}` LEFT JOIN `{next_select}`
                            ON {condition}
                    UNION
                    SELECT `{next_select}`.`{self._index_name}`,{agg_targets}
                        FROM `{next_select}` LEFT JOIN `{agg_select}`
                            ON {condition}
                            WHERE `{agg_select}`.`{self._index_name}` IS NULL"""
                if i == len(selects) - 1:
                    _kw = dict(kind=kind, keep=keep)
                else:
                    _kw = dict(kind="TABLE", keep=False)
                agg_select = enter_context(mkselect(execute, query, **_kw))
            yield agg_select, None
