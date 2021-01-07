from genefab3.sql.object import SQLiteObject
from urllib.request import urlopen


noop = lambda _:_


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, sqlite_db, table, identifier, timestamp, url, compressor, decompressor):
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
                    "timestamp": lambda value:
                        (value is None) or (timestamp > value)
                },
            },
            update={
                table: [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                    "blob": lambda: self.__download(url, compressor),
                }],
            },
            retrieve={
                table: {
                    "blob": noop if decompressor is None else decompressor,
                },
            },
        )
 
    def __download(self, url, compressor):
        with urlopen(url) as response:
            return (compressor or noop)(response.read())
