from genefab3.common.types import HashableEnough
from genefab3.sql.blob import SQLiteBlob
from urllib.request import urlopen


class CacheableBinaryFile(HashableEnough, SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, url, timestamp, sqlite_db, table="blobs", compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, url, timestamp
        self.table = table
        SQLiteBlob.__init__(
            self, data=self.__download(url),
            sqlite_db=sqlite_db, table=table,
            identifier=self.url, timestamp=self.timestamp,
            compressor=compressor, decompressor=decompressor,
        )
        HashableEnough.__init__(
            self, ("name", "url", "timestamp", "sqlite_db", "table"),
        )
 
    def __download(self, url):
        """Download data from URL as-is"""
        with urlopen(url) as response:
            return response.read()
