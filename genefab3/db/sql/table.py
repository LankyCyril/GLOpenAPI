from genefab3.db.sql.object import SQLiteObject
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.common.types import passthrough


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, data_getter, sqlite_db, table, timestamp_table, identifier, timestamp):
        if table == timestamp_table:
            raise GeneLabDatabaseException(
                "Table name cannot be equal to a reserved table name",
                table=table, identifier=identifier,
            )
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: None,
                timestamp_table: {"identifier": "TEXT", "timestamp": "INTEGER"},
            },
            trigger={
                timestamp_table: {
                    "timestamp": lambda val: (val is None) or (timestamp > val),
                },
            },
            update={
                table: [data_getter],
                timestamp_table: [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                }],
            },
            retrieve={table: passthrough},
        )
