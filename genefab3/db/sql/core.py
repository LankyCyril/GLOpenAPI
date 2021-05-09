from itertools import count
from sqlite3 import OperationalError, Binary
from genefab3.db.sql.pandas import SQLiteIndexName
from genefab3.db.sql.utils import sql_connection
from genefab3.common.utils import validate_no_backtick, validate_no_doublequote
from genefab3.common.logger import GeneFabLogger
from threading import Thread
from genefab3.common.utils import as_is
from genefab3.common.exceptions import GeneFabConfigurationException
from datetime import datetime
from genefab3.common.exceptions import GeneFabDatabaseException


class SQLiteObject():
    """Universal wrapper for cached objects"""
 
    def __init__(self, *, sqlite_db, table_schemas=None):
        """Initialize SQLiteObject, ensure tables in `sqlite_db`"""
        self.sqlite_db, self.table_schemas = sqlite_db, table_schemas
        self.changed = None
        with sql_connection(self.sqlite_db, "tables") as (_, execute):
            for table, schema in (table_schemas or {}).items():
                execute(
                    "CREATE TABLE IF NOT EXISTS `{}` ({})".format(
                        validate_no_backtick(table, "table"), ", ".join(
                            "`" + validate_no_backtick(f, "field") + "` " + k
                            for f, k in schema.items()
                        ),
                    ),
                )
 
    @classmethod
    def iterparts(cls, table, connection, *, must_exist=True, partname_mask="{table}://{i}"):
        """During an open connection, iterate all parts of `table` and their index and column names"""
        for i in count():
            partname = table if i == 0 else partname_mask.format(table=table, i=i)
            if must_exist:
                query = f"SELECT * FROM `{partname}` LIMIT 0"
                try:
                    cursor = connection.cursor()
                    cursor.execute(query).fetchall()
                except OperationalError:
                    break
                else:
                    desc = cursor.description
                    index_name = SQLiteIndexName(desc[0][0])
                    columns = [c[0] for c in desc[1:]]
                    yield partname, index_name, columns
            else:
                yield partname, None, None
 
    @classmethod
    def drop_all_parts(cls, table, connection):
        """During an open connection, drop all parts of `table`"""
        for partname, *_ in cls.iterparts(table, connection, must_exist=True):
            connection.execute(f"DROP TABLE IF EXISTS `{partname}`")
 
    @property
    def trigger(self):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        msg = "did not define self.trigger, will never update"
        GeneFabLogger().warning(f"{type(self).__name__} {msg}")
        return False
 
    def update(self):
        """Update underlying data in SQLite"""
        msg = "did not define self.update(), will never update"
        GeneFabLogger().warning(f"{type(self).__name__} {msg}")
 
    def retrieve(self):
        """Retrieve underlying data from SQLite"""
        msg = "did not define self.retrieve(), will always retrieve `None`"
        GeneFabLogger().warning(f"{type(self).__name__} {msg}")
        return None
 
    def cleanup(self):
        """Actions to be performed after request completion"""
        pass
 
    @property
    def data(self):
        """Main interface: returns data associated with this SQLiteObject; will have auto-updated itself in the process if necessary"""
        if self.trigger:
            self.update()
            self.changed = True
        else:
            self.changed = False
        Thread(target=self.cleanup).start()
        return self.retrieve()


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, *, sqlite_db, identifier, table, timestamp, data_getter, compressor=as_is, decompressor=as_is, maxdbsize=None):
        if not table.startswith("BLOBS:"):
            msg = "Table name for SQLiteBlob must start with 'BLOBS:'"
            raise GeneFabConfigurationException(msg, table=table)
        elif maxdbsize is not None:
            raise NotImplementedError("SQLiteBlob() with set `maxdbsize`")
        else:
            SQLiteObject.__init__(
                self, sqlite_db=sqlite_db, table_schemas={
                    table: {
                        "identifier": "TEXT", "blob": "BLOB",
                        "timestamp": "INTEGER", "retrieved_at": "INTEGER",
                    },
                },
            )
            self.identifier = validate_no_doublequote(identifier, "identifier")
            self.table, self.timestamp = table, timestamp
            self.data_getter = data_getter
            self.compressor, self.decompressor = compressor, decompressor
 
    def drop(self, execute):
        execute(f"""DELETE FROM `{self.table}`
            WHERE `identifier` == "{self.identifier}" """)
 
    @property
    def trigger(self):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        query = f"""SELECT `timestamp` FROM `{self.table}`
            WHERE `identifier` == "{self.identifier}" """
        with sql_connection(self.sqlite_db, "blobs") as (_, execute):
            ret = execute(query).fetchall()
            if len(ret) == 0:
                return True
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return (ret[0][0] < self.timestamp)
            else:
                msg = "Conflicting trigger values for SQLiteObject"
                GeneFabLogger().warning(f"{msg}\n  ({self.identifier})")
                self.drop()
                return True
 
    def update(self):
        """Run `self.data_getter` and insert result into `self.table` as BLOB"""
        blob = Binary(bytes(self.compressor(self.data_getter())))
        with sql_connection(self.sqlite_db, "blobs") as (_, execute):
            self.drop(execute)
            execute(f"""INSERT INTO `{self.table}`
                (`identifier`,`blob`,`timestamp`,`retrieved_at`)
                VALUES(?,?,?,?)""", [
                self.identifier, blob,
                self.timestamp, int(datetime.now().timestamp()),
            ])
 
    def retrieve(self):
        """Take `blob` from `self.table` and decompress with `self.decompressor`"""
        with sql_connection(self.sqlite_db, "blobs") as (_, execute):
            query = f"""SELECT `blob` from `{self.table}`
                WHERE `identifier` == "{self.identifier}" """
            ret = execute(query).fetchall()
            if len(ret) == 0:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, identifier=self.identifier)
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return self.decompressor(ret[0][0])
            else:
                self.drop(execute)
                msg = "Entries conflict (will attempt to fix on next request)"
                raise GeneFabDatabaseException(msg, identifier=self.identifier)
