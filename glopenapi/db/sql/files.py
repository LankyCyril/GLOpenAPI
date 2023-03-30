from glopenapi.db.sql.core import SQLiteObject, SQLiteBlob, SQLiteTable
from glopenapi.common.exceptions import GLOpenAPILogger
from requests import get as request_get
from urllib.error import URLError
from glopenapi.common.exceptions import GLOpenAPIDataManagerException
from sqlite3 import Binary, OperationalError
from datetime import datetime
from glopenapi.common.utils import as_is, random_unique_string
from contextlib import contextmanager
from os import path, remove
from shutil import copyfileobj
from csv import Error as CSVError, Sniffer
from pandas import read_csv
from pandas.errors import ParserError as PandasParserError
from pandas.io.sql import DatabaseError as PandasDatabaseError
from glopenapi.common.exceptions import GLOpenAPIFileException
from glopenapi.common.exceptions import GLOpenAPIDatabaseException
from glopenapi.common.hacks import NoCommitConnection, ExecuteMany


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
            GLOpenAPILogger.info(f"{self.name}; trying URL:\n  {url}")
            try:
                with request_get(url) as response:
                    data = response.content
            except (URLError, OSError) as exc:
                msg = f"{self.name}; tried URL and failed:\n  {url}"
                GLOpenAPILogger.warning(msg, exc_info=exc)
            else:
                msg = f"{self.name}; successfully fetched blob:\n  {url}"
                GLOpenAPILogger.info(msg)
                self.url = url
                return data
        else:
            msg = "None of the URLs are reachable for file"
            _kw = dict(name=self.name, urls=self.urls)
            raise GLOpenAPIDataManagerException(msg, **_kw)
 
    def update(self, desc="blobs/update"):
        """Run `self.__download_as_blob()` and insert result (optionally compressed) into `self.table` as BLOB"""
        blob = Binary(bytes(self.compressor(self.__download_as_blob())))
        retrieved_at = int(datetime.now().timestamp())
        with self.sqltransactions.exclusive(desc) as (connection, execute):
            if self.is_stale(ignore_conflicts=True) is False:
                return # data was updated while waiting to acquire lock
            self.drop(connection=connection)
            execute(f"""INSERT INTO `{self.table}`
                (`identifier`,`blob`,`timestamp`,`retrieved_at`)
                VALUES(?,?,?,?)""", [
                self.identifier, blob, self.timestamp, retrieved_at])
            msg = f"Inserted new blob into {self.table}"
            GLOpenAPILogger.info(f"{msg}:\n  {self.identifier}")


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
 
    @contextmanager
    def __tempfile(self):
        """Create self-destructing temporary file named by UUID"""
        filename = path.join(
            path.dirname(self.sqlite_db),
            "temp-" + random_unique_string(self.identifier) + ".raw",
        )
        try:
            yield filename
        finally:
            # Note: even with a `finally`, hot reboots / shutdowns may leave
            # some tempfiles lingering. It does not break anything, as the
            # target data will not be stored in the database, and therefore will
            # be re-downloaded using a different raw tempfile;
            # however, TODO: remove stale temp-*.raw files
            if path.isfile(filename):
                remove(filename)
 
    def __copyfileobj(self, tempfile):
        """Try all URLs and push data into temporary file"""
        for url in self.urls:
            with open(tempfile, mode="wb") as handle:
                GLOpenAPILogger.info(f"{self.name}; trying URL:\n  {url}")
                try:
                    with request_get(url, stream=True) as response:
                        response.raw.decode_content = True
                        msg = f"{self.name}:\n  streaming to {tempfile}"
                        GLOpenAPILogger.debug(msg)
                        copyfileobj(response.raw, handle)
                except (URLError, OSError) as exc:
                    msg = f"{self.name}; tried URL and failed:\n  {url}"
                    GLOpenAPILogger.warning(msg, exc_info=exc)
                else:
                    msg = f"{self.name}; successfully fetched data:\n  {url}"
                    GLOpenAPILogger.info(msg)
                    return url
        else:
            msg = "None of the URLs are reachable for file"
            _kw = dict(name=self.name, urls=self.urls)
            raise GLOpenAPIDataManagerException(msg, **_kw)
 
    def __download_as_pandas(self, chunksize, sniff_ahead=2**20):
        """Download and parse data from URL as a table"""
        with self.__tempfile() as tempfile:
            self.url = self.__copyfileobj(tempfile)
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
                    sep = Sniffer().sniff(handle.read(sniff_ahead)).delimiter
                _reader_kw = dict(
                    sep=sep, compression=compression,
                    chunksize=chunksize, **self.pandas_kws,
                )
                for i, csv_chunk in enumerate(read_csv(tempfile, **_reader_kw)):
                    self.INPLACE_process(csv_chunk)
                    msg = f"interpreted table chunk {i}:\n  {tempfile}"
                    GLOpenAPILogger.info(f"{self.name}; {msg}")
                    yield csv_chunk
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GLOpenAPIFileException(msg, name=self.name, url=self.url)
 
    def update(self, to_sql_kws=dict(index=True, if_exists="append"), chunksize=256, desc="tables/update"):
        """Update `self.table` with result of `self.__download_as_pandas()`, update `self.aux_table` with timestamps"""
        columns, width, bounds = None, None, None
        with self.sqltransactions.exclusive(desc) as (connection, execute):
            if self.is_stale(ignore_conflicts=True) is False:
                return # data was updated while waiting to acquire lock
            self.drop(connection=connection)
            for csv_chunk in self.__download_as_pandas(chunksize=chunksize):
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
                        bounded = csv_chunk.iloc[:,bound:bound+self.maxpartcols]
                        bounded.to_sql(
                            partname, NoCommitConnection(connection),
                            **to_sql_kws, chunksize=chunksize,
                            method=ExecuteMany(partname, bounded.shape[1]),
                        )
                        m = "Extended table for CachedTableFile"
                        GLOpenAPILogger.info(f"{m}:\n  {self.name}, {partname}")
                except (OperationalError, PandasDatabaseError, ValueError) as exc:
                    msg = "Failed to insert SQL chunk or chunk part"
                    _kw = dict(name=self.name, debug_info=repr(exc))
                    raise GLOpenAPIDatabaseException(msg, **_kw)
            execute(f"""INSERT INTO `{self.aux_table}`
                (`table`,`timestamp`,`retrieved_at`) VALUES(?,?,?)""", [
                self.table, self.timestamp, int(datetime.now().timestamp()),
            ])
            msg = "Finished extending; all parts inserted for CachedTableFile"
            GLOpenAPILogger.info(f"{msg}:\n  {self.name}\n  {self.table}")
