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
from pandas import read_csv, DataFrame
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
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="AUX:timestamp_table", INPLACE_process=as_is, maxpartwidth=5, **pandas_kws):
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
    """ ... """ # TODO
 
    def __init__(self, sqlite_db, column_dispatcher):
        self.sqlite_db = sqlite_db
        self.column_dispatcher = column_dispatcher
        self.parts, _pac = [], set()
        self.raw_columns, index_names = [], set()
        for n, p in column_dispatcher.items():
            if isinstance(n, SQLiteIndexName):
                index_names.add(n)
            else:
                self.raw_columns.append(n)
            if p not in _pac:
                _pac.add(p)
                self.parts.append(p)
        if len(index_names) == 0:
            msg = "OndemandSQLiteDataFrame(): no index"
            raise GeneFabDatabaseException(msg, table=self.first_part)
        elif len(index_names) > 1:
            msg = "OndemandSQLiteDataFrame(): parts indexes do not match"
            _kw = dict(table=self.first_part, index_names=index_names)
            raise GeneFabDatabaseException(msg, **_kw)
        else:
            self.index_name = index_names.pop()
        self.name = self.parts[0]
        self.columns = self.raw_columns[:]
 
    def __getitem__(self, ix):
        """ ... """ # TODO
        if (ix == slice(None)) or (ix == (slice(None), slice(None))):
            targets = ", ".join(( # TODO guard special characters here and everywhere
                f"'{self.parts[0]}'.[{self.index_name}]",
                *(f"[{c}]" for c in self.raw_columns),
            ))
            query = " ".join((
                f"SELECT {targets} FROM '{self.parts[0]}'", *(f"""
                    INNER JOIN '{part}' ON
                    '{self.parts[0]}'.[{self.index_name}] ==
                    '{part}'.[{self.index_name}]
                """ for part in self.parts[1:]),
            ))
        else:
            raise NotImplementedError("Slicing an OndemandSQLiteDataFrame")
        with closing(connect(self.sqlite_db)) as connection:
            print(query)
            try:
                cursor = connection.cursor()
                data = cursor.execute(query).fetchall()
                header = [c[0] for c in cursor.description]
            except OperationalError:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, table=self.name)
            else:
                GeneFabLogger().info(
                    "Joined %s tables for OndemandSQLiteDataFrame(): %s",
                    len(self.parts), self.name,
                )
        dataframe = DataFrame(data=data, columns=header)
        dataframe.set_index(header[0], inplace=True)
        assert header[1:] == self.raw_columns # TODO
        dataframe.columns = self.columns
        return dataframe
