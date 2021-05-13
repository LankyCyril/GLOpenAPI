from genefab3.common.utils import random_unique_string, validate_no_backtick
from sqlite3 import OperationalError
from genefab3.common.exceptions import GeneFabDatabaseException, GeneFabLogger
from genefab3.common.types import StreamedDataTable, NaN
from genefab3.common.exceptions import GeneFabFileException
from collections import Counter, OrderedDict
from collections.abc import Iterable
from re import search, sub
from genefab3.common.hacks import apply_hack, speed_up_data_schema
from genefab3.db.sql.utils import SQLTransaction


class TempSelect():
    """Temporary table or view generated from `query`"""
 
    def __init__(self, *, sqlite_db, query, targets, kind="TABLE", _depends_on=None):
        self.sqlite_db = sqlite_db
        self._depends_on = _depends_on # keeps sources from being deleted early
        self.query, self.targets, self.kind = query, targets, kind
        self.name = "TEMP:" + random_unique_string(seed=query)
        with SQLTransaction(self.sqlite_db, "tables") as (_, execute):
            try:
                execute(f"CREATE {self.kind} `{self.name}` as {query}")
            except OperationalError:
                msg = f"Failed to create temporary {self.kind}"
                _kw = dict(name=self.name, debug_info=query)
                raise GeneFabDatabaseException(msg, **_kw)
            else:
                query_repr = repr(query.lstrip()[:200] + "...")
                msg = f"Created temporary SQLite {self.kind}"
                GeneFabLogger(info=f"{msg} {self.name} from\n  {query_repr}")
 
    def __del__(self):
        with SQLTransaction(self.sqlite_db, "tables") as (_, execute):
            try:
                execute(f"DROP {self.kind} `{self.name}`")
            except OperationalError:
                msg = f"Failed to drop temporary {self.kind} {self.name}"
                GeneFabLogger(error=msg)
            else:
                msg = f"Dropped temporary SQLite {self.kind} {self.name}"
                GeneFabLogger(info=msg)


class SQLiteIndexName(str): pass


class StreamedDataTableWizard():
    """StreamedDataTable to be retrieved from SQLite, possibly from multiple parts of same or multiple tabular files"""
 
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
        if isinstance(full_name, Iterable): # string or tuple or list
            return full_name
        else: # number of occurrences in _raw_name_counts
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
        data = StreamedDataTable(
            sqlite_db=self.sqlite_db,
            source_select=self.make_select(kind="VIEW"),
            targets=",".join((
                f"`{self._index_name}`",
                *(f"`{'/'.join(c)}`" for c in self.columns),
            )),
            query_filter=self._make_query_filter(context, limit, offset),
            na_rep=NaN,
        )
        msg = f"retrieving from SQLite as StreamedDataTable"
        GeneFabLogger(info=f"{self.name};\n  {msg}")
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
 
    def make_select(self, kind):
        """Temporarily expose requested data as SQL table or view"""
        _n, _icd = len(self.columns), self._inverse_column_dispatcher
        _tt = "\n  ".join(("", *_icd))
        msg = f"retrieving {_n} columns from {len(_icd)} table(s):{_tt}"
        GeneFabLogger(info=f"{self.name}; {msg}")
        join_statement = " NATURAL JOIN ".join(f"`{p}`" for p in _icd)
        columns_as_slashed_columns = [
            f"""`{self._column_dispatcher[rawcol]}`.`{rawcol}`
                as `{self._columns_raw2slashed[rawcol]}`"""
            for *_, rawcol in self.columns
        ]
        query = f"""
            SELECT `{self._index_name}`,{','.join(columns_as_slashed_columns)}
            FROM {join_statement}"""
        return TempSelect(
            sqlite_db=self.sqlite_db, kind=kind, query=query, targets=[
                f"`{self._columns_raw2slashed[rawcol]}`"
                for *_, rawcol in self.columns
            ],
        )


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
 
    def make_select(self, kind):
        """Temporarily expose requested data as SQL table or view"""
        agg_select = self.objs[0].make_select(kind="TABLE")
        for i, obj in enumerate(self.objs[1:], 1):
            next_select = obj.make_select(kind="TABLE")
            agg_targets = agg_select.targets + next_select.targets
            query_targets = ",".join(agg_targets)
            condition = f"""`{agg_select.name}`.`{self._index_name}` ==
                `{next_select.name}`.`{self._index_name}`"""
            agg_query = f"""
                SELECT `{agg_select.name}`.`{self._index_name}`,{query_targets}
                    FROM `{agg_select.name}` LEFT JOIN `{next_select.name}`
                        ON {condition}
                UNION
                SELECT `{next_select.name}`.`{self._index_name}`,{query_targets}
                    FROM `{next_select.name}` LEFT JOIN `{agg_select.name}`
                        ON {condition}
                        WHERE `{agg_select.name}`.`{self._index_name}` ISNULL"""
            agg_select = TempSelect(
                sqlite_db=self.sqlite_db, query=agg_query, targets=agg_targets,
                kind=kind if (i == len(self.objs) - 1) else "TABLE",
                _depends_on=(agg_select, next_select),
            )
        return agg_select
