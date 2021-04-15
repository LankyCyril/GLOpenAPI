#!/usr/bin/env python3
from pandas import Index, read_sql, DataFrame
from uuid import uuid3, uuid4
from itertools import count
from contextlib import closing
from sqlite3 import connect, OperationalError


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
 
    def __retrieve_singlepart(self, index, part_to_column, columns):
        left, targets = next(iter(part_to_column.items()))
        select = ",".join(f"[{t}]" for t in targets)
        with closing(connect(self.sqlite_db)) as connection:
            try:
                data = read_sql(f"SELECT {select} FROM '{left}'", connection)
            except OperationalError:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                data.set_index(self.index.name, inplace=True)
                msg = "Read 1 table (0 joins) for OndemandSQLiteDataFrame(): %s"
                GeneFabLogger().info(msg, self.name)
                return data
 
    def __retrieve_fulljoin(self, index, part_to_column, columns):
        view_name = "VIEW:" + uuid3(uuid4(), self.name).hex
        views = [f"{view_name}:{i}" for i in range(1, len(part_to_column))]
        parts, partcols = list(part_to_column), list(part_to_column.values())
        targets = partcols[0]
        with closing(connect(self.sqlite_db)) as connection:
            try:
                cursor = connection.cursor()
                _it = zip([parts[0], *views], parts[1:], views)
                for left, right, view in _it:
                    targets.extend(part_to_column[right])
                    select = ",".join(f"[{t}]" for t in targets)
                    _sin = self.index.name
                    query = f"""CREATE VIEW '{view}' AS
                        SELECT
                            '{left}'.{select} FROM '{left}' LEFT JOIN '{right}'
                                ON '{left}'.[{_sin}] == '{right}'.[{_sin}]
                        UNION SELECT
                            '{right}'.{select} FROM '{right}' LEFT JOIN '{left}'
                                ON '{left}'.[{_sin}] == '{right}'.[{_sin}]
                            WHERE '{left}'.[{self.index.name}] IS NULL"""
                    cursor.execute(f"DROP VIEW IF EXISTS '{view}'") # TODO or guard
                    cursor.execute(query)
                data = read_sql(f"SELECT * FROM '{views[-1]}'", connection)
                for view in views:
                    cursor.execute(f"DROP VIEW IF EXISTS '{view}'")
            except OperationalError:
                connection.rollback()
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                data.set_index(self.index.name, inplace=True)
                # order may be a bit off because columns are attracted to parts:
                if (data.columns != columns).any():
                    for column in columns: # this may be kinda slow...
                        data[column] = data.pop(column)
                msg = "Joined %s tables for OndemandSQLiteDataFrame(): %s"
                GeneFabLogger().info(msg, len(parts), self.name)
                return data
 
    def get(self, *, index=None, columns=None):
        if index is not None:
            raise NotImplementedError("Slicing by index")
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
            return self.__retrieve_singlepart(index, part_to_column, columns)
        else:
            return self.__retrieve_fulljoin(index, part_to_column, columns)


from collections import OrderedDict

odf = OndemandSQLiteDataFrame(
    "test.db", OrderedDict((
        (SQLiteIndexName("entry"), "TABLE:part:0"), # TODO: no 'part' on prod
        ("B", "TABLE:part:0"),
        ("C", "TABLE:part:0"),
        ("D", "TABLE:part:1"),
        ("E", "TABLE:part:1"),
        ("F", "TABLE:part:2"),
        ("G", "TABLE:part:2"),
        ("H", "TABLE:part:3"),
        ("J", "TABLE:part:3"),
        ("K", "TABLE:part:4"),
        ("A", "TABLE:part:4"),
    )),
)

#ALL_COLUMNS = ["B", "C", "D", "E", "F", "G", "H", "J", "K", "A"]

from pandas import read_csv
from numpy.random import choice, randint
original = read_csv("D.tsv", sep="\t", index_col=0)
ALL_COLUMNS = list(original.columns)

sane = lambda df: df[sorted(set(df))].dropna(how="all").fillna("NA")

for n in range(1, 251):
    print(n, end="\r", flush=True)
    columns = choice(ALL_COLUMNS, randint(1, len(ALL_COLUMNS)), replace=False)
    colrep = " ".join((list(columns) + [" "]*len(ALL_COLUMNS))[:len(ALL_COLUMNS)])
    orig = sane(original[columns])
    try:
        data = sane(odf.get(columns=columns))
        reports = [
            colrep,
            (data.index == orig.index).all(),
            (data.columns == orig.columns).all(),
            (data == orig).all().all(),
        ]
        if not all(reports[1:]):
            print(flush=True)
            print(*reports, sep="\t")
    except Exception as e:
        print(flush=True)
        print(colrep, e, sep="\t")
