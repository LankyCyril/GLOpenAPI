from genefab3.db.sql.core import SQLiteObject, SQLiteBlob, SQLiteTable
from genefab3.common.exceptions import GeneFabLogger
from requests import get as request_get
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from sqlite3 import Binary, OperationalError
from datetime import datetime
from genefab3.common.utils import as_is, pick_reachable_url
from contextlib import contextmanager
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
        desc = "blobs/update"
        with self.LockingTierTransaction(desc) as (connection, execute):
            if self.is_stale(ignore_conflicts=True) is False:
                return # data was updated while waiting to acquire lock
            self.drop(connection=connection)
            execute(f"""INSERT INTO `{self.table}`
                (`identifier`,`blob`,`timestamp`,`retrieved_at`)
                VALUES(?,?,?,?)""", [
                self.identifier, blob, self.timestamp, retrieved_at])
            msg = f"Inserted new blob into {self.table}"
            GeneFabLogger.info(f"{msg}:\n  {self.identifier}")


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
 
    def __download_as_pandas_chunks(self, chunksize=512, sniff_ahead=2**20):
        """Download and parse data from URL as a table"""
        @contextmanager
        def _get_raw_stream(url):
            try:
                with request_get(url, stream=True) as response:
                    response.raw.decode_content = True
                    msg = f"{self.name}; streaming data:\n  {url}"
                    GeneFabLogger.info(msg)
                    yield response.raw
            except (URLError, OSError) as e:
                msg = f"{self.name}; tried URL and failed:\n  {url}"
                GeneFabLogger.warning(msg, exc_info=e)
        with pick_reachable_url(self.urls, name=self.name) as url:
            with _get_raw_stream(url) as stream:
                magic = stream.read(3)
            if magic == b"\x1f\x8b\x08":
                compression = "gzip"
            elif magic == b"\x42\x5a\x68":
                compression = "bz2"
            else:
                compression = "infer"
            try:
                with _get_raw_stream(url) as stream:
                    sniffable = stream.read(sniff_ahead).decode()
                    sep = Sniffer().sniff(sniffable).delimiter
                _reader_kw = dict(
                    sep=sep, compression=compression,
                    chunksize=chunksize, **self.pandas_kws,
                )
                for i, csv_chunk in enumerate(read_csv(url, **_reader_kw)):
                    self.INPLACE_process(csv_chunk)
                    msg = f"interpreted table chunk {i}:\n  {url}"
                    GeneFabLogger.info(f"{self.name}; {msg}")
                    yield csv_chunk
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GeneFabFileException(msg, name=self.name, url=self.url)
 
    def update(self, to_sql_kws=dict(index=True, if_exists="append", chunksize=512)):
        """Update `self.table` with result of `self.__download_as_pandas_chunks()`, update `self.aux_table` with timestamps"""
        columns, width, bounds, desc = None, None, None, "tables/update"
        with self.LockingTierTransaction(desc) as (connection, execute):
            if self.is_stale(ignore_conflicts=True) is False:
                return # data was updated while waiting to acquire lock
            self.drop(connection=connection)
            connection.commit()
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
