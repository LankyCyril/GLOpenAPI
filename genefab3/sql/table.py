from genefab3.sql.object import SQLiteObject


passthrough = lambda _:_


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, data, sqlite_db, table, timestamp_table, identifier, timestamp):
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: None,
                timestamp_table: {"identifier": "TEXT", "timestamp": "INTEGER"},
            },
            trigger={
                timestamp_table: {
                    "timestamp": lambda value:
                        (value is None) or (timestamp > value)
                },
            },
            update={
                table: [lambda: data],
                timestamp_table: [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                }],
            },
            retrieve={table: passthrough},
        )
