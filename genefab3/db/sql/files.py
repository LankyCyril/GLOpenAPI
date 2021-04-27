from genefab3.db.sql.core import SQLiteBlob, SQLiteTable
from genefab3.common.utils import as_is
from genefab3.common.logger import GeneFabLogger
from urllib.request import urlopen
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from os import path
from csv import Error as CSVError, Sniffer
from pandas import read_csv
from pandas.errors import ParserError as PandasParserError
from genefab3.common.exceptions import GeneFabFileException
 

class CachedBinaryFile(SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, table="BLOBS:blobs", compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.table = table
        SQLiteBlob.__init__(
            self, identifier=identifier, timestamp=timestamp,
            data_getter=lambda: self.__download_as_blob(urls),
            sqlite_db=sqlite_db, table=table,
            compressor=compressor, decompressor=decompressor,
        )
 
    def __download_as_blob(self, urls):
        """Download data from URL as-is"""
        self.url, data = None, None
        for url in urls:
            msg = f"{self.name}; trying URL:\n  {url}"
            GeneFabLogger().info(msg)
            try:
                with urlopen(url) as response:
                    data = response.read()
            except URLError:
                msg = f"{self.name}; tried URL and failed:\n  {url}"
                GeneFabLogger().warning(msg)
            else:
                msg = f"{self.name}; successfully fetched blob:\n  {url}"
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
        self.table = f"TABLE:{identifier}"
        SQLiteTable.__init__(
            self, identifier=f"TABLE:{identifier}", timestamp=timestamp,
            data_getter=lambda: self.__download_as_pandas_dataframe(
                urls, pandas_kws, INPLACE_process,
            ),
            sqlite_db=sqlite_db, maxpartwidth=maxpartwidth,
            table=self.table, aux_table=aux_table,
        )
 
    def __copyfileobj(self, urls, tempfile):
        """Try all URLs and push data into temporary file"""
        for url in urls:
            with open(tempfile, mode="wb") as handle:
                msg = f"{self.name}; trying URL:\n  {url}"
                GeneFabLogger().info(msg)
                try:
                    with urlopen(url) as response:
                        copyfileobj(response, handle)
                except URLError:
                    msg = f"{self.name}; tried URL and failed:\n  {url}"
                    GeneFabLogger().warning(msg)
                else:
                    msg = f"{self.name}; successfully fetched data:\n  {url}"
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
                msg = f"{self.name}; interpreted as table:\n  {tempfile}"
                GeneFabLogger().info(msg)
                INPLACE_process(dataframe)
                return dataframe
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GeneFabFileException(msg, name=self.name, url=self.url)
