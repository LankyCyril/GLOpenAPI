from genefab3.db.sql.object import SQLiteObject
from genefab3.common.types import passthrough


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
                    "blob": lambda: (compressor or passthrough)(data_getter()),
                }],
            },
            retrieve={table: {"blob": decompressor or passthrough}},
        )