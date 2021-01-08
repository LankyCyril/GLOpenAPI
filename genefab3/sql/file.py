from genefab3.common.types import HashableEnough
from genefab3.sql.blob import SQLiteBlob
from genefab3.sql.table import SQLiteTable
from urllib.request import urlopen
from tempfile import TemporaryDirectory
from shutil import copyfileobj
from os import path
from csv import Error as CSVError, Sniffer
from genefab3.common.exceptions import GeneLabFileException
from pandas import read_csv
from pandas.errors import ParserError as PandasParserError


class CacheableBinaryFile(HashableEnough, SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, url, timestamp, sqlite_db, aux_table="blobs", compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, url, timestamp
        self.aux_table = aux_table
        SQLiteBlob.__init__(
            self, identifier=url, timestamp=timestamp,
            data_getter=lambda: self.__download_as_blob(url),
            sqlite_db=sqlite_db,
            table=aux_table,
            compressor=compressor, decompressor=decompressor,
        )
        HashableEnough.__init__(
            self, ("name", "url", "timestamp", "sqlite_db", "aux_table"),
        )
 
    def __download_as_blob(self, url):
        """Download data from URL as-is"""
        with urlopen(url) as response:
            return response.read()


class CacheableTableFile(HashableEnough, SQLiteTable):
    """Represents an SQLiteObject that stores up-to-date file contents as generic table"""
 
    def __init__(self, *, name, url, timestamp, sqlite_db, aux_table="timestamp_table", **pandas_kws):
        """Interpret file descriptors; inherit functionality from SQLiteTable; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, url, timestamp
        self.aux_table = aux_table
        SQLiteTable.__init__(
            self, identifier=url, timestamp=timestamp,
            data_getter=lambda: self.__download_as_pandas_dataframe(
                url, pandas_kws,
            ),
            sqlite_db=sqlite_db,
            table=name, timestamp_table=aux_table,
        )
        HashableEnough.__init__(
            self, ("name", "url", "timestamp", "sqlite_db", "aux_table"),
        )
 
    def __download_as_pandas_dataframe(self, url, pandas_kws):
        """Download and parse data from URL as a table"""
        with TemporaryDirectory() as tempdir:
            tempfile = path.join(tempdir, self.name)
            with urlopen(url) as response, open(tempfile, mode="wb") as handle:
                copyfileobj(response, handle)
            with open(tempfile, mode="rb") as handle:
                magic = handle.read(3)
                if magic == b"\x1f\x8b\x08":
                    compression = "gzip"
                    from gzip import open as _open
                elif magic == b"\x42\x5a\x68":
                    compression = "bz2"
                    from bz2 import open as _open
                else:
                    compression = "infer"
                    _open = open
            try:
                with _open(tempfile, mode="rt", newline="") as handle:
                    sep = Sniffer().sniff(handle.read(2**20)).delimiter
                return read_csv(
                    url, sep=sep, compression=compression, **pandas_kws,
                )
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                raise GeneLabFileException(
                    "Not recognized as a table file",
                    name=self.name, url=self.url,
                )
