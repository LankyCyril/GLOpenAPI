from genefab3.db.sql.core import SQLiteObject, SQLiteBlob, SQLiteTable
from genefab3.common.exceptions import GeneFabLogger
from requests import get as request_get
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from sqlite3 import Binary, OperationalError
from datetime import datetime
from genefab3.db.sql.utils import SQLTransaction
from genefab3.common.utils import as_is
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from os import path
from csv import Error as CSVError, Sniffer
from pandas import read_csv
from pandas.errors import ParserError as PandasParserError
from pandas.io.sql import DatabaseError as PandasDatabaseError
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
            GeneFabLogger.info(f"{self.name}; trying URL:\n  {url}")
            try:
                with request_get(url) as response:
                    data = response.content
            except (URLError, OSError) as e:
                msg = f"{self.name}; tried URL and failed:\n  {url}"
                GeneFabLogger.warning(msg, exc_info=e)
            else:
                msg = f"{self.name}; successfully fetched blob:\n  {url}"
                GeneFabLogger.info(msg)
                self.url = url
                return data
        else:
            msg = "None of the URLs are reachable for file"
            _kw = dict(name=self.name, urls=self.urls)
            raise GeneFabDataManagerException(msg, **_kw)
 
    def update(self):
        """Run `self.__download_as_blob()` and insert result (optionally compressed) into `self.table` as BLOB"""
        blob = Binary(bytes(self.compressor(self.__download_as_blob())))
        retrieved_at = int(datetime.now().timestamp())
        _kw = dict(desc="blobs/update", exclusive=1)
        with SQLTransaction(self.sqlite_db, **_kw) as (connection, execute):
            self.drop(connection=connection)
            execute(f"""INSERT INTO `{self.table}`
                (`identifier`,`blob`,`timestamp`,`retrieved_at`)
                VALUES(?,?,?,?)""", [
                self.identifier, blob, self.timestamp, retrieved_at])


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
                GeneFabLogger.info(f"{self.name}; trying URL:\n  {url}")
                try:
                    with request_get(url, stream=True) as response:
                        response.raw.decode_content = False
                        msg = f"{self.name}:\n  streaming to {handle.name}"
                        GeneFabLogger.debug(msg)
                        copyfileobj(response.raw, handle)
                except (URLError, OSError) as e:
                    msg = f"{self.name}; tried URL and failed:\n  {url}"
                    GeneFabLogger.warning(msg, exc_info=e)
                else:
                    msg = f"{self.name}; successfully fetched data:\n  {url}"
                    GeneFabLogger.info(msg)
                    return url
        else:
            msg = "None of the URLs are reachable for file"
            raise GeneFabDataManagerException(msg, name=self.name, urls=urls)
 
    def __download_as_pandas_chunks(self, chunksize=1024):
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
                _reader_kw = dict(
                    sep=sep, compression=compression,
                    chunksize=chunksize, **self.pandas_kws,
                )
                for i, csv_chunk in enumerate(read_csv(tempfile, **_reader_kw)):
                    self.INPLACE_process(csv_chunk)
                    msg = f"interpreted table chunk {i}:\n  {tempfile}"
                    GeneFabLogger.info(f"{self.name}; {msg}")
                    yield csv_chunk
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GeneFabFileException(msg, name=self.name, url=self.url)
 
    def update(self, to_sql_kws=dict(index=True, if_exists="append", chunksize=1024)):
        """Update `self.table` with result of `self.__download_as_pandas_chunks()`, update `self.aux_table` with timestamps"""
        columns, width, bounds = None, None, None
        _kw = dict(desc="tables/update", exclusive=1)
        with SQLTransaction(self.sqlite_db, **_kw) as (connection, execute):
            for csv_chunk in self.__download_as_pandas_chunks():
                try:
                    columns = csv_chunk.columns if columns is None else columns
                    if width is None:
                        width = csv_chunk.shape[1]
                        bounds = bounds or range(0, width, self.maxpartcols)
                    if (csv_chunk.shape[1] != width):
                        raise ValueError("Inconsistent chunk width")
                    if (csv_chunk.columns != columns).any():
                        raise ValueError("Inconsistent chunk column names")
                    parts = SQLiteObject.iterparts(
                        self.table, connection, must_exist=0,
                    )
                    for bound, (partname, *_) in zip(bounds, parts):
                        csv_chunk.iloc[:,bound:bound+self.maxpartcols].to_sql(
                            partname, connection, **to_sql_kws,
                        )
                        msg = "Extended table for CachedTableFile"
                        GeneFabLogger.info(f"{msg}:\n  {self.name}, {partname}")
                except (OperationalError, PandasDatabaseError, ValueError) as e:
                    msg = "Failed to insert SQLite chunk (or chunk part)"
                    GeneFabLogger.error(f"{msg}:\n  {self.name}", exc_info=e)
                    connection.rollback()
                    return
            execute(f"""INSERT INTO `{self.aux_table}`
                (`table`,`timestamp`,`retrieved_at`) VALUES(?,?,?)""", [
                self.table, self.timestamp, int(datetime.now().timestamp()),
            ])
            msg = "Finished extending; all parts inserted for CachedTableFile"
            GeneFabLogger.info(f"{msg}:\n  {self.name}\n  {self.table}")
