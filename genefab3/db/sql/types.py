from genefab3.db.sql.objects import SQLiteObject
from genefab3.common.utils import as_is
from genefab3.common.exceptions import GeneFabConfigurationException
from collections import OrderedDict
from urllib.request import urlopen
from urllib.error import URLError
from genefab3.common.logger import GeneFabLogger
from genefab3.common.exceptions import GeneFabDataManagerException
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from os import path
from csv import Error as CSVError, Sniffer
from pandas import read_csv, read_sql, DataFrame, Index, MultiIndex
from pandas.errors import ParserError as PandasParserError
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDatabaseException
from contextlib import closing
from sqlite3 import connect, OperationalError


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, data_getter, sqlite_db, table, identifier, timestamp, compressor, decompressor):
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: {
                    "identifier": "TEXT",
                    "timestamp": "INTEGER",
                    "blob": "BLOB",
                },
            },
            trigger={
                table: {
                    "timestamp": lambda val: (val is None) or (timestamp > val),
                },
            },
            update={
                table: [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                    "blob": lambda: (compressor or as_is)(data_getter()),
                }],
            },
            retrieve={table: {"blob": decompressor or as_is}},
        )


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, data_getter, sqlite_db, table, aux_table, identifier, timestamp, maxpartwidth=1000):
        if table == aux_table:
            msg = "Table name cannot be equal to a reserved table name"
            _kw = dict(table=table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
        self.maxpartwidth = maxpartwidth
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: None,
                aux_table: {"identifier": "TEXT", "timestamp": "INTEGER"},
            },
            trigger={
                aux_table: {
                    "timestamp": lambda val: (val is None) or (timestamp > val),
                },
            },
            update=OrderedDict((
                (table, [data_getter]),
                (aux_table, [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                }]),
            )),
            retrieve={table: as_is},
        )
 

class CachedBinaryFile(SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="BLOBS:blobs", compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.aux_table = aux_table
        SQLiteBlob.__init__(
            self, identifier=identifier, timestamp=timestamp,
            data_getter=lambda: self.__download_as_blob(urls),
            sqlite_db=sqlite_db,
            table=aux_table,
            compressor=compressor, decompressor=decompressor,
        )
 
    def __download_as_blob(self, urls):
        """Download data from URL as-is"""
        self.url, data = None, None
        for url in urls:
            msg = f"{self.name}; trying URL: {url}"
            GeneFabLogger().info(msg)
            try:
                with urlopen(url) as response:
                    data = response.read()
            except URLError:
                msg = f"{self.name}; tried URL and failed: {url}"
                GeneFabLogger().warning(msg)
            else:
                msg = f"{self.name}; successfully fetched blob: {url}"
                GeneFabLogger().info(msg)
                self.url = url
                return data
        else:
            msg = "None of the URLs are reachable for file"
            raise GeneFabDataManagerException(msg, name=self.name, urls=urls)


class CachedTableFile(SQLiteTable):
    """Represents an SQLiteObject that stores up-to-date file contents as generic table"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="AUX:timestamp_table", INPLACE_process=as_is, maxpartwidth=1000, **pandas_kws):
        """Interpret file descriptors; inherit functionality from SQLiteTable; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.aux_table = aux_table
        SQLiteTable.__init__(
            self, identifier=f"TABLE:{identifier}", timestamp=timestamp,
            data_getter=lambda: self.__download_as_pandas_dataframe(
                urls, pandas_kws, INPLACE_process,
            ),
            sqlite_db=sqlite_db, maxpartwidth=maxpartwidth,
            table=f"TABLE:{identifier}", aux_table=aux_table,
        )
 
    def __copyfileobj(self, urls, tempfile):
        """Try all URLs and push data into temporary file"""
        for url in urls:
            with open(tempfile, mode="wb") as handle:
                msg = f"{self.name}; trying URL: {url}"
                GeneFabLogger().info(msg)
                try:
                    with urlopen(url) as response:
                        copyfileobj(response, handle)
                except URLError:
                    msg = f"{self.name}; tried URL and failed: {url}"
                    GeneFabLogger().warning(msg)
                else:
                    msg = f"{self.name}; successfully fetched data: {url}"
                    GeneFabLogger().info(msg)
                    return url
        else:
            msg = "None of the URLs are reachable for file"
            raise GeneFabDataManagerException(msg, name=self.name, urls=urls)
 
    def __download_as_pandas_dataframe(self, urls, pandas_kws, INPLACE_process=as_is):
        """Download and parse data from URL as a table"""
        with TemporaryDirectory() as tempdir:
            tempfile = path.join(tempdir, self.name)
            self.url = self.__copyfileobj(urls, tempfile)
            with open(tempfile, mode="rb") as handle:
                magic = handle.read(3)
            if magic == b"\x1f\x8b\x08":
                compression = "gzip"
                from gzip import open as _open
            elif magic == b"\x42\x5a\x68":
                compression = "bz2"
                from bz2 import open as _open
            else:
                compression, _open = "infer", open
            try:
                with _open(tempfile, mode="rt", newline="") as handle:
                    sep = Sniffer().sniff(handle.read(2**20)).delimiter
                dataframe = read_csv(
                    tempfile, sep=sep, compression=compression, **pandas_kws,
                )
                msg = f"{self.name}; interpreted as table: {tempfile}"
                GeneFabLogger().info(msg)
                INPLACE_process(dataframe)
                return dataframe
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GeneFabFileException(msg, name=self.name, url=self.url)


class SQLiteIndexName(str): pass


class OndemandSQLiteDataFrame():
    """DataFrame to be retrieved from SQLite, possibly from multiple tables, including parts of same tabular file"""
 
    def __init__(self, sqlite_db, column_dispatcher, columns=None):
        """Interpret `column_dispatcher`"""
        self.sqlite_db = sqlite_db
        self._column_dispatcher = column_dispatcher
        _parts, _pac = [], set()
        self._raw_columns, _index_names = [], set()
        for n, p in column_dispatcher.items():
            if isinstance(n, SQLiteIndexName):
                _index_names.add(n)
            else:
                self._raw_columns.append(n)
            if p not in _pac:
                _pac.add(p)
                _parts.append(p)
        if len(_index_names) == 0:
            msg = "OndemandSQLiteDataFrame(): no index"
            raise GeneFabDatabaseException(msg, table=_parts[0])
        elif len(_index_names) > 1:
            msg = "OndemandSQLiteDataFrame(): parts indexes do not match"
            _kw = dict(table=_parts[0], index_names=_index_names)
            raise GeneFabDatabaseException(msg, **_kw)
        self.name = _parts[0]
        self.index = Index([], name=_index_names.pop())
        if columns is None:
            self._columns = Index(self._raw_columns, name=None)
        else:
            r, c = len(self._raw_columns), len(columns)
            if r == c:
                self._columns = columns
            else:
                m = f"Passed `columns` have {c} elements, data has {r} columns"
                raise ValueError(f"Length mismatch: {m}")
 
    @property
    def columns(self): return self._columns
    @columns.setter
    def columns(self, value):
        c, v = len(self._columns), len(value)
        if c == v:
            self._columns = value
        else:
            m = f"Expected axis has {c} elements, new values have {v} elements"
            raise ValueError(f"Length mismatch: {m}")
 
    def get(self, *, rows=None, columns=None, limit=None, offset=0):
        """Interpret arguments in order to retrieve data as DataFrame by running SQL queries"""
        if (offset != 0) and (limit is None):
            msg = "OndemandSQLiteDataFrame: `offset` without `limit`"
            raise GeneFabDatabaseException(msg, table=self.name)
        if rows is not None:
            raise NotImplementedError("Slicing by row names")
        if columns is not None:
            _cs = set(columns)
            if len(_cs) != len(columns):
                raise IndexError(f"Slicing by duplicate column name")
            elif self.index.name in _cs:
                raise IndexError(f"Requesting index as if it were a column")
        else:
            columns = self._raw_columns
        part_to_column = OrderedDict({
            # get index column from part containing first requested column:
            self._column_dispatcher[columns[0]]: [self.index.name],
        })
        for column in (self._raw_columns if columns is None else columns):
            part = self._column_dispatcher[column]
            part_to_column.setdefault(part, []).append(column)
        if len(part_to_column) == 0:
            return DataFrame()
        else:
            args = rows, part_to_column, columns, limit, offset
            return self.__retrieve_natural_join(*args)
 
    def __retrieve_natural_join(self, rows, part_to_column, columns, limit, offset):
        """Retrieve data as DataFrame by running SQL queries"""
        joined = " NATURAL JOIN ".join(f"'{p}'" for p in part_to_column)
        first_table = next(iter(part_to_column))
        targets = ",".join((
            f"'{first_table}'.[{self.index.name}]",
            *(f"[{c}]" for c in columns),
        ))
        with closing(connect(self.sqlite_db)) as connection:
            _n, _m = len(columns), len(part_to_column)
            _tt = "\n\t".join(("", *part_to_column))
            msg = f"retrieving {_n} columns from {_m} table(s):{_tt}"
            GeneFabLogger().info(f"{self.name}; {msg}")
            try:
                if limit is None:
                    query = f"SELECT {targets} FROM {joined}"
                else:
                    _limits = f"LIMIT {limit} OFFSET {offset}"
                    query = f"SELECT {targets} FROM {joined} {_limits}"
                data = read_sql(query, connection, index_col=self.index.name)
            except OperationalError:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                data.columns = self.columns
                return data
 
    @staticmethod
    def concat(objs, axis=1):
        """Concatenate OndemandSQLiteDataFrame objects without evaluation"""
        _t = "OndemandSQLiteDataFrame"
        if axis != 1:
            raise ValueError(f"{_t}.concat(..., axis={axis}) makes no sense")
        elif not all(isinstance(obj, OndemandSQLiteDataFrame) for obj in objs):
            raise TypeError(f"{_t}.concat() on non-{_t} objects")
        elif len(set(obj.sqlite_db for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects from different database files"
            raise ValueError(msg)
        elif len(set(obj.index.name for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing index names"
            raise ValueError(msg)
        elif len(set(obj.columns.nlevels for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing column `nlevels`"
            raise ValueError(msg)
        else:
            column_dispatcher = OrderedDict()
            for obj in objs:
                for n, p in obj._column_dispatcher.items():
                    if n in column_dispatcher:
                        if not isinstance(n, SQLiteIndexName):
                            e = f"duplicate column name: {n}"
                            msg = f"Cannot merge multiple {_t} objects with {e}"
                            raise IndexError(msg)
                    else:
                        column_dispatcher[n] = p
            if objs[0].columns.nlevels == 1:
                _mkindex = Index
            else:
                _mkindex = MultiIndex.from_tuples
            columns = _mkindex(sum((obj.columns.to_list() for obj in objs), []))
            return OndemandSQLiteDataFrame(
                objs[0].sqlite_db, column_dispatcher, columns,
            )
