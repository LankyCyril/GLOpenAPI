from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from os import access, W_OK, path
from genefab3.common.utils import iterate_terminal_leaves, as_is
from itertools import count
from genefab3.db.sql.pandas import SQLiteIndexName
from functools import lru_cache, partial
from pandas import isnull, DataFrame
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.utils import validate_no_backtick, validate_no_doublequote
from copy import deepcopy
from genefab3.common.logger import GeneFabLogger
from pandas.io.sql import DatabaseError
from genefab3.common.exceptions import GeneFabDatabaseException
from collections.abc import Callable
from collections import OrderedDict
from genefab3.db.sql.pandas import OndemandSQLiteDataFrame_Single
from threading import Thread
from datetime import datetime


def is_sqlite_file_ready(filename):
    """Make sure `filename` is reachable and writable, set auto_vacuum to 1 (FULL)"""
    try:
        with closing(connect(filename)) as connection:
            connection.cursor().execute("PRAGMA auto_vacuum = 1")
    except (OSError, FileNotFoundError, OperationalError):
        return False
    else: # if not writable, but already on auto_vacuum = 1, won't have thrown
        return access(filename, W_OK)


def is_singular_spec(spec):
    """Check if a dictionary passed to SQLiteObject() has only one entry"""
    try:
        if not isinstance(spec, dict):
            return False
        else:
            return (sum(1 for _ in iterate_terminal_leaves(spec)) == 1)
    except ValueError:
        return False


def iterparts(table, connection, must_exist=True, partname_mask="{table}://{i}"):
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


@lru_cache(maxsize=16384)
def format_sql_value(value):
    """Format known singleton values according to how SQLite understands them"""
    if isnull(value):
        return "null"
    elif isinstance(value, bool):
        return str(int(value))
    elif isinstance(value, float):
        if value == float("inf"):
            return "9e999"
        elif value == -float("inf"):
            return "-9e999"
        else:
            return repr(value)
    else:
        return repr(value)


def mkinsert(pd_table, conn, keys, data_iter, name):
    """Non-parameterized SQLite INSERT for simple table schemas from trusted sources"""
    for row in data_iter:
        values = ",".join(format_sql_value(v) for v in row)
        conn.execute(f"INSERT INTO `{name}` VALUES({values})")


def drop_all_parts(table, connection):
    """During an open connection, drop all parts of `table`"""
    cursor = connection.cursor()
    for partname, *_ in iterparts(table, connection):
        cursor.execute(f"DROP TABLE IF EXISTS `{partname}`")


class SQLiteObject():
    """Universal wrapper for cached objects; defined by table schemas, the update/retrieve spec, and the re-cache trigger condition"""
 
    def __init__(self, sqlite_db, signature, table_schemas, trigger, update, retrieve):
        """Parse the update/retrieve spec and the re-cache trigger condition; create tables if they do not exist"""
        self.sqlite_db, self.__table_schemas = sqlite_db, table_schemas
        if len(signature) != 1:
            msg = "SQLiteObject(): Only one 'identifier' field can be specified"
            raise GeneFabConfigurationException(msg, signatures=signature)
        else:
            self.__identifier_field = validate_no_backtick(
                next(iter(signature)), "__identifier_field",
            )
            self.__identifier_value = validate_no_doublequote(
                next(iter(signature.values())), "__identifier_value",
            )
            try:
                self.__signature = deepcopy(signature)
            except ValueError:
                msg = "SQLiteObject(): Bad signature"
                raise GeneFabConfigurationException(msg, signature=signature)
        if sqlite_db is not None:
            for table, schema in table_schemas.items():
                if schema is not None:
                    self.__ensure_table(table, schema)
                else:
                    validate_no_backtick(table, "table")
        try:
            self.__trigger_spec = deepcopy(trigger)
            self.__retrieve_spec = deepcopy(retrieve)
            self.__update_spec = deepcopy(update)
        except ValueError:
            msg = "SQLiteObject(): Bad spec"
            _kw = dict(trigger=trigger, update=update, retrieve=retrieve)
            raise GeneFabConfigurationException(msg, **_kw)
        self.changed = None
 
    def __ensure_table(self, table, schema):
        """Create table with schema, provided as a dictionary"""
        with closing(connect(self.sqlite_db)) as connection:
            connection.cursor().execute(
                "CREATE TABLE IF NOT EXISTS `{}` ({})".format(
                    validate_no_backtick(table, "table"), ", ".join(
                        "`" + validate_no_backtick(f, "field") + "` " + k
                        for f, k in schema.items()
                    ),
                ),
            )
 
    def __update_fields(self, table, spec, trigger_field, trigger_value):
        """Update table field(s) in SQLite and drop `trigger_field` (which should be replaced according to spec)"""
        fields, values = sorted(spec), []
        for field in (validate_no_backtick(f, "field") for f in fields):
            value = spec[field]()
            if self.__table_schemas[table][field] == "BLOB":
                values.append(Binary(bytes(value)))
            else:
                values.append(value)
        delete_action = f"""DELETE FROM `{table}`
            WHERE `{trigger_field}` == "{trigger_value}"
            AND `{self.__identifier_field}` == "{self.__identifier_value}" """
        insert_action = f"""INSERT INTO `{table}` (`{"`, `".join(fields)}`)
            VALUES ({", ".join("?" for _ in fields)})"""
        with closing(connect(self.sqlite_db)) as connection:
            logger_args = self.__identifier_field, self.__identifier_value
            try:
                connection.cursor().execute(delete_action)
                connection.cursor().execute(insert_action, values)
                msg = "Updated fields for SQLiteObject\n  (%s == %s)"
                GeneFabLogger().info(msg, *logger_args)
            except OperationalError:
                msg = "Could not update fields for SQLiteObject\n  (%s == %s)"
                GeneFabLogger().warning(msg, *logger_args)
                connection.rollback()
            else:
                connection.commit()
 
    def __update_table(self, table, spec):
        """Update table(s) in SQLite"""
        dataframe = spec()
        if not isinstance(dataframe, DataFrame):
            msg = "Cached table not represented as pandas DataFrame(s)"
            raise NotImplementedError(msg)
        elif dataframe.index.nlevels != 1:
            msg = "MultiIndex in cached DataFrame"
            raise NotImplementedError(msg)
        elif dataframe.columns.nlevels != 1:
            msg = "MultiIndex columns in cached DataFrame"
            raise NotImplementedError(msg)
        with closing(connect(self.sqlite_db)) as connection:
            bounds = range(0, dataframe.shape[1], self.maxpartwidth)
            part_iterator = iterparts(table, connection, must_exist=False)
            for bound, (partname, *_) in zip(bounds, part_iterator):
                GeneFabLogger().info(
                    "Creating table for SQLiteObject (%s == %s):\n  %s",
                    self.__identifier_field, self.__identifier_value,
                    partname,
                )
                try:
                    dataframe.iloc[:,bound:bound+self.maxpartwidth].to_sql(
                        partname, connection, index=True, if_exists="replace",
                        chunksize=1000, method=partial(mkinsert, name=partname),
                    )
                except (OperationalError, DatabaseError) as e:
                    drop_all_parts(table, connection)
                    connection.rollback()
                    msg = "Failed to insert SQLite table"
                    _kw = dict(signature=self.__signature, error=str(e))
                    raise GeneFabDatabaseException(msg, **_kw)
            else:
                GeneFabLogger().info(
                    "All tables inserted for SQLiteObject (%s == %s):\n  %s",
                    self.__identifier_field, self.__identifier_value, partname,
                )
                connection.commit()
 
    def __update(self, trigger_field, trigger_value):
        """Update table or table field in SQLite and drop `trigger_field` (to be replaced according to spec)"""
        for table, specs in self.__update_spec.items():
            for spec in specs:
                if isinstance(spec, dict):
                    self.__update_fields(
                        table, spec, trigger_field, trigger_value,
                    )
                elif isinstance(spec, Callable):
                    self.__update_table(table, spec)
                else:
                    msg = "SQLiteObject: unsupported type of update spec"
                    raise GeneFabConfigurationException(msg, type=type(spec))
 
    def __drop_self_from(self, connection, table):
        """Helper method (during an open connection) to drop rows matching `self.signature` from `table`"""
        try:
            connection.cursor().execute(f"""DELETE FROM `{table}` WHERE
                `{self.__identifier_field}` == "{self.__identifier_value}" """)
        except OperationalError:
            msg = "Could not drop multiple entries for same %s == %s"
            logger_args = self.__identifier_field, self.__identifier_value
            GeneFabLogger().error(msg, *logger_args)
 
    def __retrieve_field(self, table, field, postprocess_function):
        """Retrieve target table field from database"""
        with closing(connect(self.sqlite_db)) as connection:
            query = f"""SELECT `{field}` from `{table}` WHERE
                `{self.__identifier_field}` == "{self.__identifier_value}" """
            ret = connection.cursor().execute(query).fetchall()
            if len(ret) == 0:
                msg = "No data found"
                raise GeneFabDatabaseException(msg, signature=self.__signature)
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return postprocess_function(ret[0][0])
            else:
                self.__drop_self_from(connection, table)
                msg = "Entries conflict (will attempt to fix on next request)"
                raise GeneFabDatabaseException(msg, signature=self.__signature)
 
    def __retrieve_table(self, table, postprocess_function=as_is):
        """Create an OndemandSQLiteDataFrame object dispatching columns to table parts"""
        column_dispatcher = OrderedDict()
        with closing(connect(self.sqlite_db)) as connection:
            for partname, index_name, columns in iterparts(table, connection):
                if index_name not in column_dispatcher:
                    column_dispatcher[index_name] = partname
                for c in columns:
                    column_dispatcher[c] = partname
        if not column_dispatcher:
            msg = "No data found"
            raise GeneFabDatabaseException(msg, signature=self.__signature)
        else:
            return postprocess_function(OndemandSQLiteDataFrame_Single(
                self.sqlite_db, column_dispatcher,
            ))
 
    def __retrieve(self):
        """Retrieve target table or table field from database"""
        if not is_singular_spec(self.__retrieve_spec):
            msg = "SQLiteObject(): Only one 'retrieve' field can be specified"
            raise GeneFabConfigurationException(msg, signature=self.__signature)
        else:
            table, spec = next(iter(self.__retrieve_spec.items()))
            if isinstance(spec, dict):
                field = next(iter(spec))
                postprocess_function = spec[field]
                return self.__retrieve_field(table, field, postprocess_function)
            elif isinstance(spec, Callable):
                return self.__retrieve_table(table, postprocess_function=spec)
            else:
                msg = "SQLiteObject: unsupported type of retrieve spec"
                raise GeneFabConfigurationException(msg, type=type(spec))
 
    def deferrable_cleanup(self):
        """Actions to be performed after request completion"""
        pass
 
    def __conditional_update(self, table_or_aux, trigger_field, trigger_function):
        """Check trigger fields and values and perform update if triggered"""
        with closing(connect(self.sqlite_db)) as connection:
            query = f"""SELECT `{trigger_field}` FROM `{table_or_aux}` WHERE
                `{self.__identifier_field}` == "{self.__identifier_value}" """
            ret = connection.cursor().execute(query).fetchall()
            if len(ret) == 0:
                trigger_value = None
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                trigger_value = validate_no_doublequote(
                    ret[0][0], "trigger_value",
                )
            else:
                m = "Conflicting trigger values for SQLiteObject\n  (%s == %s)"
                logger_args = self.__identifier_field, self.__identifier_value
                GeneFabLogger().warning(m, *logger_args)
                self.__drop_self_from(connection, table_or_aux)
                trigger_value = None
        if trigger_function(trigger_value):
            self.changed = True
            self.__update(trigger_field, trigger_value)
        else:
            self.changed = False
        Thread(target=self.deferrable_cleanup).start()
 
    @property
    def data(self):
        """Main interface: returns data associated with this SQLiteObject; will have auto-updated itself in the process if necessary"""
        if not is_singular_spec(self.__trigger_spec):
            msg = "SQLiteObject(): Only one 'trigger' field can be specified"
            raise GeneFabConfigurationException(msg, signature=self.__signature)
        else:
            table_or_aux = validate_no_backtick(
                next(iter(self.__trigger_spec)), "table or aux_table",
            )
            trigger_field = validate_no_backtick(
                next(iter(self.__trigger_spec[table_or_aux])), "trigger_field",
            )
            trigger_function = self.__trigger_spec[table_or_aux][trigger_field]
        self.__conditional_update(
            table_or_aux=table_or_aux,
            trigger_field=trigger_field, trigger_function=trigger_function,
        )
        return self.__retrieve()


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, data_getter, sqlite_db, table, identifier, timestamp, compressor, decompressor, maxdbsize=None):
        if not table.startswith("BLOBS:"):
            msg = "Table name for SQLiteBlob must start with 'BLOBS:'"
            _kw = dict(table=table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
        if maxdbsize is not None:
            raise NotImplementedError("SQLiteBlob() with set `maxdbsize`")
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: {
                    "identifier": "TEXT",
                    "timestamp": "INTEGER",
                    "retrieved_at": "INTEGER",
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
                    "retrieved_at": lambda: int(datetime.now().timestamp()),
                    "blob": lambda: (compressor or as_is)(data_getter()),
                }],
            },
            retrieve={table: {"blob": decompressor or as_is}},
        )


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, data_getter, sqlite_db, table, aux_table, identifier, timestamp, maxpartwidth=1000, maxdbsize=None):
        if not table.startswith("TABLE:"):
            msg = "Table name for SQLiteTable must start with 'TABLE:'"
            _kw = dict(table=table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
        if not aux_table.startswith("AUX:"):
            msg = "Aux table name for SQLiteTable must start with 'AUX:'"
            _kw = dict(aux_table=aux_table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
        self.identifier, self.aux_table = identifier, aux_table
        self.maxpartwidth, self.maxdbsize = maxpartwidth, maxdbsize
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: None,
                aux_table: {
                    "identifier": "TEXT",
                    "timestamp": "INTEGER",
                    "retrieved_at": "INTEGER",
                },
            },
            trigger={
                aux_table: {
                    "timestamp": lambda val: (val is None) or (timestamp > val),
                },
            },
            update=OrderedDict((
                (table, [data_getter]),
                (aux_table, [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                    "retrieved_at": lambda: int(datetime.now().timestamp()),
                }]),
            )),
            retrieve={table: as_is},
        )
 
    def deferrable_cleanup(self, max_iter=100, max_skids=20):
        """Check size of underlying database file, drop oldest tables to keep file size under `self.maxdbsize`"""
        # TODO: this is a little unclean, because it hard-codes / repeats field names from specs in SQLiteTable.__init__(),
        # TODO  as well as repeating logic from genefab3.db.sql.response_cache ResponseCache.shrink()
        desc = f"SQLiteTable():\n  {self.sqlite_db}"
        target_size = self.maxdbsize or float("inf")
        n_dropped, n_skids = 0, 0
        for _ in range(max_iter):
            current_size = path.getsize(self.sqlite_db)
            if (n_skids < max_skids) and (current_size > target_size):
                GeneFabLogger().info(f"{desc} is being shrunk")
                with closing(connect(self.sqlite_db)) as connection:
                    query_oldest = f"""SELECT `identifier`,`retrieved_at`
                        FROM `{self.aux_table}` WHERE `retrieved_at` ==
                        (SELECT MIN(`retrieved_at`) FROM `{self.aux_table}`)
                        LIMIT 1"""
                    try:
                        cursor = connection.cursor()
                        entries = cursor.execute(query_oldest).fetchall()
                        if len(entries) and (len(entries[0]) == 2):
                            identifier = entries[0][0]
                        else:
                            break
                        cursor.execute(f"""DELETE FROM `{self.aux_table}`
                            WHERE `identifier` == "{identifier}" """)
                        drop_all_parts(identifier, connection)
                    except OperationalError:
                        connection.rollback()
                        break
                    else:
                        connection.commit()
                        n_dropped += 1
                n_skids += (path.getsize(self.sqlite_db) >= current_size)
            else:
                break
        is_too_big = (path.getsize(self.sqlite_db) > target_size)
        self._report_cleanup(desc, n_dropped, is_too_big, n_skids)
 
    def _report_cleanup(self, desc, n_dropped, is_too_big, n_skids):
        if n_dropped:
            GeneFabLogger().info(f"{desc} shrunk by {n_dropped} entries")
        elif is_too_big:
            GeneFabLogger().warning(f"{desc} could not be shrunk")
        if n_skids:
            GeneFabLogger().warning(f"{desc} did not shrink {n_skids} times")
