from genefab3.db.sql.core import SQLiteObject, SQLiteBlob, SQLiteTable
from genefab3.common.exceptions import GeneFabLogger
from urllib.request import urlopen
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from sqlite3 import Binary, OperationalError
from genefab3.db.sql.utils import SQLTransaction
from datetime import datetime
from genefab3.common.utils import as_is
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from os import path
from csv import Error as CSVError, Sniffer
from pandas import read_csv
from pandas.errors import ParserError as PandasParserError
from pandas.io.sql import DatabaseError as PandasDatabaseError
from genefab3.common.exceptions import GeneFabDatabaseException
from genefab3.common.exceptions import GeneFabFileException
 

class CachedBinaryFile(SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, table="BLOBS:blobs", compressor=None, decompressor=None, maxdbsize=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name = name
        self.url, self.urls = None, urls
        SQLiteBlob.__init__(
            self, sqlite_db=sqlite_db, maxdbsize=maxdbsize,
            table=table, identifier=identifier, timestamp=timestamp,
            compressor=compressor, decompressor=decompressor,
        )
 
    def __download_as_blob(self):
        """Download data from URL as-is"""
        for url in self.urls:
            GeneFabLogger(info=f"{self.name}; trying URL:\n  {url}")
            try:
                with urlopen(url) as response:
                    data = response.read()
            except URLError:
                msg = f"{self.name}; tried URL and failed:\n  {url}"
                GeneFabLogger(warning=msg)
            else:
                msg = f"{self.name}; successfully fetched blob:\n  {url}"
                GeneFabLogger(info=msg)
                self.url = url
                return data
        else:
            msg = "None of the URLs are reachable for file"
            _kw = dict(name=self.name, urls=self.urls)
            raise GeneFabDataManagerException(msg, **_kw)
 
    def update(self):
        """Run `self.__download_as_blob()` and insert result (optionally compressed) into `self.table` as BLOB"""
        blob = Binary(bytes(self.compressor(self.__download_as_blob())))
        with SQLTransaction(self.sqlite_db, "blobs") as (connection, execute):
            self.drop(connection=connection)
            execute(f"""INSERT INTO `{self.table}`
                (`identifier`,`blob`,`timestamp`,`retrieved_at`)
                VALUES(?,?,?,?)""", [
                self.identifier, blob,
                self.timestamp, int(datetime.now().timestamp()),
            ])


class CachedTableFile(SQLiteTable):
    """Represents an SQLiteObject that stores up-to-date file contents as generic table"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="AUX:timestamp_table", INPLACE_process=as_is, maxdbsize=None, **pandas_kws):
        """Interpret file descriptors; inherit functionality from SQLiteTable; define equality (hashableness) of self"""
        self.name, self.identifier = name, identifier
        self.url, self.urls = None, urls
        self.pandas_kws, self.INPLACE_process = pandas_kws, INPLACE_process
        SQLiteTable.__init__(
            self, sqlite_db=sqlite_db, maxdbsize=maxdbsize,
            table=identifier, aux_table=aux_table, timestamp=timestamp,
        )
 
    def __copyfileobj(self, urls, tempfile):
        """Try all URLs and push data into temporary file"""
        for url in urls:
            with open(tempfile, mode="wb") as handle:
                GeneFabLogger(info=f"{self.name}; trying URL:\n  {url}")
                try:
                    with urlopen(url) as response:
                        copyfileobj(response, handle)
                except URLError:
                    msg = f"{self.name}; tried URL and failed:\n  {url}"
                    GeneFabLogger(warning=msg)
                else:
                    msg = f"{self.name}; successfully fetched data:\n  {url}"
                    GeneFabLogger(info=msg)
                    return url
        else:
            msg = "None of the URLs are reachable for file"
            raise GeneFabDataManagerException(msg, name=self.name, urls=urls)
 
    def __download_as_pandas_dataframe(self):
        """Download and parse data from URL as a table"""
        with TemporaryDirectory() as tempdir:
            tempfile = path.join(tempdir, self.name)
            self.url = self.__copyfileobj(self.urls, tempfile)
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
                    tempfile, sep=sep, compression=compression,
                    **self.pandas_kws,
                )
                msg = f"{self.name}; interpreted as table:\n  {tempfile}"
                GeneFabLogger(info=msg)
                self.INPLACE_process(dataframe)
                return dataframe
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GeneFabFileException(msg, name=self.name, url=self.url)
 
    def update(self, to_sql_kws=dict(index=True, if_exists="replace", chunksize=1024)):
        """Update `self.table` with result of `self.__download_as_pandas_dataframe()`, update `self.aux_table` with timestamps"""
        dataframe = self.__download_as_pandas_dataframe()
        with SQLTransaction(self.sqlite_db, "tables") as (connection, execute):
            self.drop(connection=connection)
            bounds = range(0, dataframe.shape[1], self.maxpartcols)
            parts = SQLiteObject.iterparts(self.table, connection, must_exist=0)
            try:
                for bound, (partname, *_) in zip(bounds, parts):
                    msg = "Creating table for SQLiteObject"
                    GeneFabLogger(info=f"{msg}:\n  {partname}")
                    dataframe.iloc[:,bound:bound+self.maxpartcols].to_sql(
                        partname, connection, **to_sql_kws,
                    )
                execute(f"""INSERT INTO `{self.aux_table}`
                    (`table`,`timestamp`,`retrieved_at`) VALUES(?,?,?)""", [
                    self.table, self.timestamp, int(datetime.now().timestamp()),
                ])
            except (OperationalError, PandasDatabaseError) as e:
                self.drop(connection=connection)
                msg = "Failed to insert SQLite table (or table part)"
                _kw = dict(part=partname, debug_info=repr(e))
                raise GeneFabDatabaseException(msg, **_kw)
            else:
                msg = "All tables inserted for SQLiteObject"
                GeneFabLogger(info=f"{msg}:\n  {self.table}")
