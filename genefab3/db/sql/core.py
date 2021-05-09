from itertools import count
from sqlite3 import OperationalError, Binary
from genefab3.db.sql.streamed_tables import SQLiteIndexName
from genefab3.db.sql.utils import sql_connection
from genefab3.common.utils import validate_no_backtick, validate_no_doublequote
from genefab3.common.logger import GeneFabLogger
from threading import Thread
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.utils import as_is
from datetime import datetime
from genefab3.common.exceptions import GeneFabDatabaseException
from math import inf
from pandas import DataFrame
from collections import OrderedDict
from genefab3.db.sql.streamed_tables import StreamedDataTableWizard_Single
from pandas.io.sql import DatabaseError as PandasDatabaseError
from os import path


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
 
    def is_stale(self, *, timestamp_table=None, id_field=None, db_type=None):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        if (timestamp_table is None) or (id_field is None):
            msg = "did not pass arguments to self.is_stale(), will never update"
            GeneFabLogger().warning(f"{type(self).__name__} {msg}")
        else:
            desc = db_type or f"for {type(self).__name__}"
            self_id_value = getattr(self, id_field)
            query = f"""SELECT `timestamp` FROM `{timestamp_table}`
                WHERE `{id_field}` == "{self_id_value}" """
        with sql_connection(self.sqlite_db, desc) as (connection, execute):
            ret = execute(query).fetchall()
            if len(ret) == 0:
                return True
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return (ret[0][0] < self.timestamp)
            else:
                msg = "Conflicting timestamp values for SQLiteObject"
                GeneFabLogger().warning(f"{msg}\n  ({self_id_value})")
                self.drop(connection=connection)
                return True
 
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
        if self.is_stale():
            self.update()
            self.changed = True
        else:
            self.changed = False
        Thread(target=self.cleanup).start()
        return self.retrieve()


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, *, sqlite_db, identifier, table, timestamp, data_getter, compressor=None, decompressor=None, maxdbsize=None):
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
            self.compressor = compressor or as_is
            self.decompressor = decompressor or as_is
 
    def drop(self, *, connection, other=None):
        identifier = other or self.identifier
        connection.execute(f"""DELETE FROM `{self.table}`
            WHERE `identifier` == "{identifier}" """)
 
    def is_stale(self):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        return SQLiteObject.is_stale(
            self, timestamp_table=self.table, id_field="identifier",
            db_type="blobs",
        )
 
    def update(self):
        """Run `self.data_getter` and insert result into `self.table` as BLOB"""
        blob = Binary(bytes(self.compressor(self.data_getter())))
        with sql_connection(self.sqlite_db, "blobs") as (connection, execute):
            self.drop(connection=connection)
            execute(f"""INSERT INTO `{self.table}`
                (`identifier`,`blob`,`timestamp`,`retrieved_at`)
                VALUES(?,?,?,?)""", [
                self.identifier, blob,
                self.timestamp, int(datetime.now().timestamp()),
            ])
 
    def retrieve(self):
        """Take `blob` from `self.table` and decompress with `self.decompressor`"""
        with sql_connection(self.sqlite_db, "blobs") as (connection, execute):
            query = f"""SELECT `blob` from `{self.table}`
                WHERE `identifier` == "{self.identifier}" """
            ret = execute(query).fetchall()
            if len(ret) == 0:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, identifier=self.identifier)
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return self.decompressor(ret[0][0])
            else:
                self.drop(connection=connection)
                msg = "Entries conflict (will attempt to fix on next request)"
                raise GeneFabDatabaseException(msg, identifier=self.identifier)


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, *, sqlite_db, table, aux_table, timestamp, data_getter, maxpartcols=998, maxdbsize=None):
        if not table.startswith("TABLE:"):
            msg = "Table name for SQLiteTable must start with 'TABLE:'"
            raise GeneFabConfigurationException(msg, table=table)
        elif not aux_table.startswith("AUX:"):
            msg = "Aux table name for SQLiteTable must start with 'AUX:'"
            raise GeneFabConfigurationException(msg, aux_table=aux_table)
        else:
            SQLiteObject.__init__(
                self, sqlite_db=sqlite_db, table_schemas={
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
            self.data_getter = data_getter
 
    def drop(self, *, connection, other=None):
        table = other or self.table
        connection.execute(f"""DELETE FROM `{self.aux_table}`
            WHERE `table` == "{table}" """)
        SQLiteObject.drop_all_parts(table, connection)
 
    def is_stale(self):
        """Evaluates to True if underlying data in need of update, otherwise False"""
        return SQLiteObject.is_stale(
            self, timestamp_table=self.aux_table, id_field="table",
            db_type="tables",
        )
 
    def _validated_dataframe(self, dataframe):
        if not isinstance(dataframe, DataFrame):
            msg = "Cached table not represented as pandas DataFrame(s)"
            raise NotImplementedError(msg)
        elif dataframe.index.nlevels != 1:
            msg = "MultiIndex in cached DataFrame"
            raise NotImplementedError(msg)
        elif dataframe.columns.nlevels != 1:
            msg = "MultiIndex columns in cached DataFrame"
            raise NotImplementedError(msg)
        else:
            return dataframe
 
    def update(self, to_sql_kws=dict(index=True, if_exists="replace", chunksize=1024)):
        """Update `self.table` with result of `self.data_getter()`, update `self.aux_table` with timestamps"""
        dataframe = self._validated_dataframe(self.data_getter())
        with sql_connection(self.sqlite_db, "tables") as (connection, execute):
            self.drop(connection=connection)
            bounds = range(0, dataframe.shape[1], self.maxpartcols)
            parts = SQLiteObject.iterparts(self.table, connection, must_exist=0)
            try:
                for bound, (partname, *_) in zip(bounds, parts):
                    msg = "Creating table for SQLiteObject"
                    GeneFabLogger().info(f"{msg}:\n  {partname}")
                    dataframe.iloc[:,bound:bound+self.maxpartcols].to_sql(
                        partname, connection, **to_sql_kws,
                    )
                execute(f"""INSERT INTO `{self.aux_table}`
                    (`table`,`timestamp`,`retrieved_at`) VALUES(?,?,?)""", [
                    self.table, self.timestamp, int(datetime.now().timestamp()),
                ])
            except (OperationalError, PandasDatabaseError) as e:
                self.drop(connection=connection)
                msg = "Failed to insert SQLite table (or table part)"
                _kw = dict(part=partname, debug_info=repr(e))
                raise GeneFabDatabaseException(msg, **_kw)
            else:
                msg = "All tables inserted for SQLiteObject"
                GeneFabLogger().info(f"{msg}:\n  {self.table}")
 
    def retrieve(self):
        """Create an StreamedDataTableWizard object dispatching columns to table parts"""
        column_dispatcher = OrderedDict()
        with sql_connection(self.sqlite_db, "tables") as (connection, _):
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
                self.sqlite_db, column_dispatcher,
            )
 
    def cleanup(self, max_iter=100, max_skids=20):
        """Check size of underlying database file, drop oldest tables to keep file size under `self.maxdbsize`"""
        desc, n_dropped, n_skids = f"SQLiteTable():\n  {self.sqlite_db}", 0, 0
        for _ in range(max_iter):
            current_size = path.getsize(self.sqlite_db)
            if (n_skids < max_skids) and (current_size > self.maxdbsize):
                GeneFabLogger().info(f"{desc} is being shrunk")
                _kw = dict(filename=self.sqlite_db, desc="tables")
                with sql_connection(**_kw) as (connection, execute):
                    query_oldest = f"""SELECT `table`
                        FROM `{self.aux_table}` WHERE `retrieved_at` ==
                        (SELECT MIN(`retrieved_at`) FROM `{self.aux_table}`)
                        LIMIT 1"""
                    try:
                        table = (execute(query_oldest).fetchone() or [None])[0]
                        if table is None:
                            break
                        self.drop(connection=connection, other=table)
                    except OperationalError:
                        connection.rollback()
                        break
                    else:
                        connection.commit()
                        n_dropped += 1
                n_skids += (path.getsize(self.sqlite_db) >= current_size)
            else:
                break
        if n_dropped:
            GeneFabLogger().info(f"{desc} shrunk by {n_dropped} entries")
        elif (path.getsize(self.sqlite_db) > self.maxdbsize):
            GeneFabLogger().warning(f"{desc} could not be shrunk")
        if n_skids:
            GeneFabLogger().warning(f"{desc} did not shrink {n_skids} times")
