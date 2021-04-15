#!/usr/bin/env python3
from pandas import Index, read_sql
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
 
    def __retrieve(self, index, part_to_column):
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
        return self.__retrieve(index, part_to_column)


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

from numpy.random import choice, randint
ALL_COLUMNS = ["B", "C", "D", "E", "F", "G", "H", "J", "K", "A"]
COLUMNS = choice(ALL_COLUMNS, randint(len(ALL_COLUMNS)), replace=False)
print(COLUMNS, end="\n\n")

from pandas import read_csv
orig = read_csv("D.tsv", sep="\t", index_col=0)[COLUMNS]

try:
    print("Getting data...")
    data = odf.get(columns=COLUMNS)
except Exception as e:
    print("Exception occurred:", e)

try:
    print("Columns match:", (data.columns == orig.columns).all())
    print("Indexes match:", (data.index == orig.index).all())
    print("Values match:", (data.fillna("NA") == orig.fillna("NA")).all().all())
except Exception as e:
    print("Exception occurred:", e)
    print(orig, end="\n\n")
    print(data)
