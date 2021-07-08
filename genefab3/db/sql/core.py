from genefab3.db.sql.utils import SQLTransactions
from genefab3.common.utils import validate_no_backtick, validate_no_doublequote
from itertools import count
from sqlite3 import OperationalError
from genefab3.db.sql.streamed_tables import SQLiteIndexName
from genefab3.common.exceptions import GeneFabLogger
from threading import Thread
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.utils import as_is
from genefab3.common.exceptions import GeneFabDatabaseException
from math import inf
from collections import OrderedDict
from genefab3.db.sql.streamed_tables import StreamedDataTableWizard_Single
from os import path


class SQLiteObject():
    """Universal wrapper for cached objects"""
 
    def __init__(self, *, sqlite_db, identifier=None, table_schemas=None):
        """Initialize SQLiteObject, ensure tables in `sqlite_db`"""
        self.sqlite_db, self.table_schemas = sqlite_db, table_schemas
        self.identifier = identifier
        self.changed = None
        self.sqltransactions = SQLTransactions(sqlite_db, identifier)
        desc = "tables/ensure_schema"
        with self.sqltransactions.concurrent(desc) as (_, execute):
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
                    index_name = SQLiteIndexName(cursor.description[0][0])
                    columns = [c[0] for c in cursor.description[1:]]
                    yield partname, index_name, columns
            else:
                yield partname, None, None
 
    @classmethod
    def drop_all_parts(cls, table, connection):
        """During an open connection, drop all parts of `table`"""
        _iterparts = cls.iterparts(table, connection, must_exist=True)
        for partname, *_ in list(_iterparts):
            try:
                connection.execute(f"DROP TABLE IF EXISTS `{partname}`")
            except Exception as e:
                GeneFabLogger.error(f"Could not drop {partname}", exc_info=e)
                raise
            else:
                GeneFabLogger.info(f"Dropped {partname} (if it existed)")
 
    def is_stale(self, *, timestamp_table=None, id_field=None, db_type=None, ignore_conflicts=False):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        if (timestamp_table is None) or (id_field is None):
            msg = "did not pass arguments to self.is_stale(), will never update"
            GeneFabLogger.warning(f"{type(self).__name__} {msg}")
        else:
            db_type = db_type or f"{type(self).__name__}"
            desc = f"{db_type}/is_stale"
            self_id_value = getattr(self, id_field)
            query = f"""SELECT `timestamp` FROM `{timestamp_table}`
                WHERE `{id_field}` == "{self_id_value}" """
        with self.sqltransactions.concurrent(desc) as (_, execute):
            ret = execute(query).fetchall()
            if len(ret) == 0:
                _staleness = True
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                _staleness = (ret[0][0] < self.timestamp)
            else:
                _staleness = None
        if (_staleness is None) and (not ignore_conflicts):
            with self.sqltransactions.exclusive(desc) as (connection, _):
                msg = "Conflicting timestamp values for SQLiteObject"
                GeneFabLogger.warning(f"{msg}\n  ({self_id_value})")
                self.drop(connection=connection)
            _staleness = True
        if _staleness is True:
            GeneFabLogger.info(f"{self_id_value} is stale, staging update")
        return _staleness
 
    def update(self):
        """Update underlying data in SQLite"""
        msg = "did not define self.update(), will never update"
        GeneFabLogger.warning(f"{type(self).__name__} {msg}")
 
    def retrieve(self):
        """Retrieve underlying data from SQLite"""
        msg = "did not define self.retrieve(), will always retrieve `None`"
        GeneFabLogger.warning(f"{type(self).__name__} {msg}")
        return None
 
    def cleanup(self):
        """Actions to be performed after request completion"""
        pass
 
    @property
    def data(self):
        """Main interface: returns data associated with this SQLiteObject; will have auto-updated itself in the process if necessary"""
        if self.is_stale():
            self.update()
            self.changed = True
        else:
            self.changed = False
        Thread(target=self.cleanup).start()
        return self.retrieve()


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, *, sqlite_db, identifier, table, timestamp, compressor=None, decompressor=None, maxdbsize=None):
        if not table.startswith("BLOBS:"):
            msg = "Table name for SQLiteBlob must start with 'BLOBS:'"
            raise GeneFabConfigurationException(msg, table=table)
        elif maxdbsize is not None:
            raise NotImplementedError("SQLiteBlob() with set `maxdbsize`")
        else:
            SQLiteObject.__init__(
                self, sqlite_db=sqlite_db,
                identifier=validate_no_doublequote(identifier, "identifier"),
                table_schemas={
                    table: {
                        "identifier": "TEXT", "blob": "BLOB",
                        "timestamp": "INTEGER", "retrieved_at": "INTEGER",
                    },
                },
            )
            self.table, self.timestamp = table, timestamp
            self.compressor = compressor or as_is
            self.decompressor = decompressor or as_is
 
    def drop(self, *, connection, other=None):
        identifier = other or self.identifier
        try:
            connection.execute(f"""DELETE FROM `{self.table}`
                WHERE `identifier` == "{identifier}" """)
        except Exception as e:
            msg = f"Could not delete from {self.table}: {identifier}"
            GeneFabLogger.error(msg, exc_info=e)
            raise
        else:
            GeneFabLogger.info(f"Deleted from {self.table}: {identifier}")
 
    def is_stale(self, ignore_conflicts=False):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        return SQLiteObject.is_stale(
            self, timestamp_table=self.table, id_field="identifier",
            db_type="blobs", ignore_conflicts=ignore_conflicts,
        )
 
    def retrieve(self, desc="blobs/retrieve"):
        """Take `blob` from `self.table` and decompress with `self.decompressor`"""
        with self.sqltransactions.concurrent(desc) as (_, execute):
            query = f"""SELECT `blob` from `{self.table}`
                WHERE `identifier` == "{self.identifier}" """
            ret = execute(query).fetchall()
            if len(ret) == 0:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, identifier=self.identifier)
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                data = self.decompressor(ret[0][0])
            else:
                data = None
        if data is None:
            with self.sqltransactions.concurrent(desc) as (connection, _):
                self.drop(connection=connection)
                msg = "Entries conflict (will attempt to fix on next request)"
                raise GeneFabDatabaseException(msg, identifier=self.identifier)
        else:
            return data


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, *, sqlite_db, table, aux_table, timestamp, maxpartcols=998, maxdbsize=None):
        if not table.startswith("TABLE:"):
            msg = "Table name for SQLiteTable must start with 'TABLE:'"
            raise GeneFabConfigurationException(msg, table=table)
        elif not aux_table.startswith("AUX:"):
            msg = "Aux table name for SQLiteTable must start with 'AUX:'"
            raise GeneFabConfigurationException(msg, aux_table=aux_table)
        else:
            self.table = validate_no_backtick(
                validate_no_doublequote(table, "table"), "table",
            )
            SQLiteObject.__init__(
                self, sqlite_db=sqlite_db, identifier=self.table,
                table_schemas={
                    aux_table: {
                        "table": "TEXT",
                        "timestamp": "INTEGER", "retrieved_at": "INTEGER",
                    },
                },
            )
            self.table = validate_no_backtick(
                validate_no_doublequote(table, "table"), "table",
            )
            self.aux_table, self.timestamp = aux_table, timestamp
            self.maxpartcols, self.maxdbsize = maxpartcols, maxdbsize or inf
 
    def drop(self, *, connection, other=None):
        table = other or self.table
        try:
            connection.execute(f"""DELETE FROM `{self.aux_table}`
                WHERE `table` == "{table}" """)
        except Exception as e:
            msg = f"Could not delete from {self.aux_table}: {table}"
            GeneFabLogger.error(msg, exc_info=e)
            raise
        else:
            GeneFabLogger.info(f"Deleted from {self.aux_table}: {table}")
        SQLiteObject.drop_all_parts(table, connection)
 
    def is_stale(self, ignore_conflicts=False):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        return SQLiteObject.is_stale(
            self, timestamp_table=self.aux_table, id_field="table",
            db_type="tables", ignore_conflicts=ignore_conflicts,
        )
 
    def retrieve(self, desc="tables/retrieve"):
        """Create an StreamedDataTableWizard object dispatching columns to table parts"""
        column_dispatcher = OrderedDict()
        with self.sqltransactions.concurrent(desc) as (connection, _):
            parts = SQLiteObject.iterparts(self.table, connection)
            for partname, index_name, columns in parts:
                if index_name not in column_dispatcher:
                    column_dispatcher[index_name] = partname
                for c in columns:
                    column_dispatcher[c] = partname
        if not column_dispatcher:
            raise GeneFabDatabaseException("No data found", table=self.table)
        else:
            return StreamedDataTableWizard_Single(
                self.sqlite_db, column_dispatcher, identifier=self.identifier,
            )
 
    def cleanup(self, max_iter=100, max_skids=20, desc="tables/cleanup"):
        """Check size of underlying database file, drop oldest tables to keep file size under `self.maxdbsize`"""
        n_dropped, n_skids = 0, 0
        for _ in range(max_iter):
            current_size = path.getsize(self.sqlite_db)
            if (n_skids < max_skids) and (current_size > self.maxdbsize):
                with self.sqltransactions.concurrent(desc) as (_, execute):
                    query_oldest = f"""SELECT `table`
                        FROM `{self.aux_table}` ORDER BY `retrieved_at` ASC"""
                    table = (execute(query_oldest).fetchone() or [None])[0]
                    if table is None:
                        break
                with self.sqltransactions.exclusive(desc) as (connection, _):
                    try:
                        GeneFabLogger.info(f"{desc} purging: {table}")
                        self.drop(connection=connection, other=table)
                    except OperationalError as e:
                        msg= f"Rolling back shrinkage due to {e!r}"
                        GeneFabLogger.error(msg, exc_info=e)
                        connection.rollback()
                        break
                    else:
                        connection.commit()
                        n_dropped += 1
                n_skids += (path.getsize(self.sqlite_db) >= current_size)
            else:
                break
        desc = f"SQLiteTable():\n  {self.sqlite_db}"
        if n_dropped:
            GeneFabLogger.info(f"{desc} shrunk by {n_dropped} entries")
        elif path.getsize(self.sqlite_db) > self.maxdbsize:
            GeneFabLogger.warning(f"{desc} could not be shrunk")
        if n_skids:
            GeneFabLogger.warning(f"{desc} did not shrink {n_skids} times")
