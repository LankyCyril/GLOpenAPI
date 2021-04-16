#!/usr/bin/env python3
from contextlib import contextmanager, closing
from datetime import datetime
from pandas import Index, read_sql, DataFrame, concat
from uuid import uuid3, uuid4
from itertools import count
from sqlite3 import connect, OperationalError


@contextmanager
def timed(desc=None):
    t = datetime.timestamp(datetime.now())
    timer = lambda: None
    yield timer
    timer.delta = datetime.timestamp(datetime.now()) - t
    if desc is not None:
        print(f"{desc} took {timer.delta}")


class SQLiteIndexName(str): pass


def GeneFabLogger():
    from logging import getLogger
    return getLogger()


class OndemandSQLiteDataFrame():
 
    def __init__(self, sqlite_db, column_dispatcher):
        self.sqlite_db = sqlite_db
        self.__column_dispatcher = column_dispatcher
        self.__parts, _pac = [], set()
        _columns, _index_names = [], set()
        for n, p in column_dispatcher.items():
            if isinstance(n, SQLiteIndexName):
                _index_names.add(n)
            else:
                _columns.append(n)
            if p not in _pac:
                _pac.add(p)
                self.__parts.append(p)
        if len(_index_names) == 0:
            msg = "OndemandSQLiteDataFrame(): no index"
            raise GeneFabDatabaseException(msg, table=self.__parts[0])
        elif len(_index_names) > 1:
            msg = "OndemandSQLiteDataFrame(): parts indexes do not match"
            _kw = dict(table=self.__parts[0], index_names=_index_names)
            raise GeneFabDatabaseException(msg, **_kw)
        self.name = self.__parts[0]
        self.index = Index([], name=_index_names.pop())
        self.columns = Index(_columns, name=None)
 
    def __retrieve_singlepart(self, rows, left, targets, limit, offset):
        select = ",".join(f"[{t}]" for t in targets)
        with closing(connect(self.sqlite_db)) as connection:
            query = f"SELECT {select} FROM '{left}'"
            try:
                if limit is None:
                    data = read_sql(query, connection)
                else:
                    limits = f"LIMIT {limit} OFFSET {offset}"
                    data = read_sql(f"{query} {limits}", connection)
            except OperationalError:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                data.set_index(self.index.name, inplace=True)
                msg = "Read 1 table (0 joins) for OndemandSQLiteDataFrame(): %s"
                GeneFabLogger().info(msg, self.name)
                return data
 
    def __retrieve_full_join(self, rows, part_to_column, columns, limit, offset, sort_rows, sort_columns):
        if (offset != 0) and (limit is None):
            msg = "OndemandSQLiteDataFrame(): `offset` without `limit`"
            raise GeneFabDatabaseException(msg, table=self.name)
        view_name = "VIEW:" + uuid3(uuid4(), self.name).hex
        views = [f"{view_name}:{i}" for i in range(1, len(part_to_column))]
        parts, partcols = list(part_to_column), list(part_to_column.values())
        _sin, targets = self.index.name, partcols[0]
        union_op = "UNION" if sort_rows else "UNION ALL"
        with closing(connect(self.sqlite_db)) as connection:
            try:
                cursor = connection.cursor()
                _it = zip([parts[0], *views], parts[1:], views)
                for left, right, view in _it:
                    targets.extend(part_to_column[right])
                    select = ",".join(f"[{t}]" for t in targets)
                    query = f"""CREATE VIEW '{view}' AS
                        SELECT
                            '{left}'.{select} FROM '{left}' LEFT JOIN '{right}'
                                ON '{left}'.[{_sin}] == '{right}'.[{_sin}]
                        {union_op} SELECT
                            '{right}'.{select} FROM '{right}' LEFT JOIN '{left}'
                                ON '{left}'.[{_sin}] == '{right}'.[{_sin}]
                            WHERE '{left}'.[{self.index.name}] IS NULL"""
                    cursor.execute(f"DROP VIEW IF EXISTS '{view}'") # TODO or guard
                    cursor.execute(query)
                query = f"SELECT * FROM '{views[-1]}'"
                if limit is None:
                    data = read_sql(query, connection)
                else:
                    limits = f"LIMIT {limit} OFFSET {offset}"
                    data = read_sql(f"{query} {limits}", connection)
                for view in views:
                    cursor.execute(f"DROP VIEW IF EXISTS '{view}'")
            except OperationalError:
                connection.rollback()
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                data.set_index(self.index.name, inplace=True)
                # order may be a bit off because columns are attracted to parts:
                if sort_columns and (data.columns != columns).any():
                    for column in columns: # this may be kinda slow...
                        data[column] = data.pop(column)
                msg = "Joined %s tables for OndemandSQLiteDataFrame(): %s"
                GeneFabLogger().info(msg, len(parts), self.name)
                return data
 
    def __retrieve_natural_join(self, rows, part_to_column, columns, limit, offset, sort_rows, sort_columns):
        """ https://stackoverflow.com/a/12095198 """
        if (offset != 0) and (limit is None):
            msg = "OndemandSQLiteDataFrame(): `offset` without `limit`"
            raise GeneFabDatabaseException(msg, table=self.name)
        elif sort_rows:
            msg = "OndemandSQLiteDataFrame(): `sort_rows` without JOIN"
            raise GeneFabDatabaseException(msg, table=self.name)
        left = next(iter(part_to_column))
        _sel = ",".join(
            (f"'{left}'.[{self.index.name}]", *(f"[{c}]" for c in columns)),
        )
        tables = " NATURAL JOIN ".join(f"'{p}'" for p in part_to_column)
        if limit is None:
            query = f"SELECT {_sel} FROM {tables}"
        else:
            query = f"SELECT {_sel} FROM {tables} LIMIT {limit} OFFSET {offset}"
        with closing(connect(self.sqlite_db)) as connection:
            try:
                return read_sql(query, connection, index_col=self.index.name)
            except OperationalError:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
 
    def get(self, *, rows=None, columns=None, limit=None, offset=0, sort_rows=True, sort_columns=True, full_join=True, pandas_concat=False):
        if rows is not None:
            raise NotImplementedError("Slicing by row names")
        if columns is not None:
            _cs = set(columns)
            if len(_cs) != len(columns):
                raise IndexError(f"Slicing by duplicate column name")
            elif self.index.name in _cs:
                raise IndexError(f"Requesting index as if it were a column")
        else:
            columns = self.columns
        part_to_column = OrderedDict({
            # get index column from part containing first requested column:
            self.__column_dispatcher[columns[0]]: [self.index.name],
        })
        for column in (self.columns if columns is None else columns):
            part = self.__column_dispatcher[column]
            part_to_column.setdefault(part, []).append(column)
        if len(part_to_column) == 0:
            return DataFrame()
        elif len(part_to_column) == 1:
            left, targets = next(iter(part_to_column.items()))
            args = rows, left, targets, limit, offset
            return self.__retrieve_singlepart(*args)
        elif pandas_concat: # TODO this block is here only for speed tests
            parts = []
            for left, targets in part_to_column.items():
                if targets[0] != self.index.name:
                    targets = [self.index.name, *targets]
                args = rows, left, targets, limit, offset
                parts.append(self.__retrieve_singlepart(*args))
            return concat(parts, axis=1, sort=sort_rows)
        elif full_join:
            args = rows, part_to_column, columns, limit, offset
            return self.__retrieve_full_join(*args, sort_rows, sort_columns)
        else:
            args = rows, part_to_column, columns, limit, offset
            return self.__retrieve_natural_join(*args, sort_rows, sort_columns)


from pickle import load, dump
from numpy.random import choice, randint, random_integers
from numpy import nan
from collections import OrderedDict

W, H = 3777, 50000
MAXPARTWIDTH = 500
MAXCOL = min(100, W)
COMPARE = False

with closing(connect("sample.db")) as connection:
    try:
        connection.cursor().execute("SELECT * FROM sample LIMIT 1")
        print("Loading pre-generated data", flush=True)
        with open("sample.pkl", mode="rb") as pkl:
            original = load(pkl)
    except (OperationalError, FileNotFoundError):
        print(f"Creating {W}x{H} DataFrame", flush=True)
        A = random_integers(20, size=(H, W)).astype(float)
        A[A<7] = nan
        original = DataFrame(
            columns=[f"C{i}" for i in range(W)],
            index=Index([f"R{i}" for i in range(H)], name="rolling"),
            data=A,
        )
        print(f"Writing {W}x{H} DataFrame to PKL", flush=True)
        with open("sample.pkl", mode="wb") as pkl:
            dump(original, pkl)
        print(f"Writing SQL parts of {W}x{H} DataFrame", flush=True)
        for j, i in enumerate(range(0, W, MAXPARTWIDTH)):
            original.iloc[:,i:i+MAXPARTWIDTH].to_sql(
                "sample" if i == 0 else f"sample:{j}",
                connection, index=True, if_exists="replace",
            )
        print("Done with data generation", end="\n\n", flush=True)
    else:
        print("Found and loaded pre-generated data", end="\n\n", flush=True)

column_dispatcher = OrderedDict({SQLiteIndexName("rolling"): "sample"})
for i in range(W):
    if i < MAXPARTWIDTH:
        column_dispatcher[f"C{i}"] = "sample"
    else:
        pn = i // MAXPARTWIDTH
        column_dispatcher[f"C{i}"] = f"sample:{pn}"
odf = OndemandSQLiteDataFrame("sample.db", column_dispatcher)


ALL_COLUMNS = list(original.columns)
sane = lambda d: d.loc[sorted(d.index), sorted(d)].dropna(how="all").fillna("?")


if False:
    columns = choice(ALL_COLUMNS, randint(1, MAXCOL), replace=False)
    data = odf.get(columns=columns, limit=5, full_join=False, sort_rows=False)
    print(data)
    data = original[columns][:5]
    print(data)


if False:
    _kw_default = dict(sort_columns=False, sort_rows=False)
    _kw_hstack = dict(**_kw_default, full_join=False)
    _kw_pandas = dict(**_kw_default, pandas_concat=True)
    columns = [f"C{i}" for i in range(500)]
    with timed(desc="SQL join single-part"):
        _ = odf.get(columns=columns, **_kw_default)
    with timed(desc="SQL stack single-part"):
        _ = odf.get(columns=columns, **_kw_hstack)
    with timed(desc="Pandas join single-part"):
        _ = odf.get(columns=columns, **_kw_pandas)
    columns = [f"C{i}" for i in range(250, 750)]
    with timed(desc="SQL join double-part"):
        _ = odf.get(columns=columns, **_kw_default)
    with timed(desc="SQL stack double-part"):
        _ = odf.get(columns=columns, **_kw_hstack)
    with timed(desc="Pandas join double-part"):
        _ = odf.get(columns=columns, **_kw_pandas)
    columns = (
        [f"C{i}" for i in range(350, 550)] +
        [f"C{i}" for i in range(950, 1050)] +
        [f"C{i}" for i in range(1950, 2150)]
    )
    with timed(desc="SQL join triple-part"):
        _ = odf.get(columns=columns, **_kw_default)
    with timed(desc="SQL stack triple-part"):
        _ = odf.get(columns=columns, **_kw_hstack)
    with timed(desc="Pandas join triple-part"):
        _ = odf.get(columns=columns, **_kw_pandas)


if False:
    _kw_default = dict(sort_columns=False, sort_rows=False)
    _kw_hstack = dict(**_kw_default, full_join=False)
    _kw_pandas = dict(**_kw_default, pandas_concat=True)
    columns = (
        [f"C{i}" for i in range(350, 550)] +
        [f"C{i}" for i in range(950, 1050)] +
        [f"C{i}" for i in range(1950, 2150)]
    )
    with timed(desc="SQL join triple-part"):
        a = odf.get(columns=columns, **_kw_default)
    with timed(desc="SQL stack triple-part"):
        b = odf.get(columns=columns, **_kw_hstack)
    with timed(desc="Pandas join triple-part"):
        c = odf.get(columns=columns, **_kw_pandas)
    sane_d = sane(original[columns][::999])
    print((sane_d == sane(a[::999])).all().all())
    print((sane_d == sane(b[::999])).all().all())
    print((sane_d == sane(c[::999])).all().all())


if True:
    for n in range(1, 10):
        columns = choice(ALL_COLUMNS, randint(1, MAXCOL), replace=False)
        print(f"len(columns) == {len(columns)}", flush=True)
        if COMPARE:
            orig = sane(original[columns])
            sort_rows, sort_columns = True, True
        else:
            sort_rows, sort_columns = False, False
        try:
            with timed(desc="full_join") as timer:
                data = odf.get(columns=columns, sort_rows=False)
            with timed(desc="natural") as timer:
                data = odf.get(columns=columns, full_join=False, sort_rows=False)
            if COMPARE:
                data = sane(data)
                reports = [
                    (data.index == orig.index).all(),
                    (data.columns == orig.columns).all(),
                    (data == orig).all().all(),
                ]
                if not all(reports):
                    print(flush=True)
                    print(list(columns))
                    print("", *reports, sep="\t")
        except Exception as e:
            print(list(columns))
            print("\t{e}")
        else:
            print("\tAll good")
