from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from os import access, W_OK
from genefab3.common.utils import iterate_terminal_leaves, as_is
from pandas import isnull, DataFrame
from genefab3.common.utils import validate_no_backtick, validate_no_doublequote
from genefab3.common.exceptions import GeneFabConfigurationException
from copy import deepcopy
from genefab3.common.logger import GeneFabLogger
from functools import partial
from collections.abc import Callable
from genefab3.db.sql.pandas import SQLiteIndexName
from collections import OrderedDict
from genefab3.db.sql.pandas import OndemandSQLiteDataFrame_Single
from genefab3.common.exceptions import GeneFabDatabaseException
from pandas.io.sql import DatabaseError
from itertools import count


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


def mkinsert(pd_table, conn, keys, data_iter, name):
    """SQLite INSERT without variable names for simple table schemas"""
    for row in data_iter:
        vals = ",".join("null" if isnull(v) else repr(v) for v in row)
        conn.execute(f"INSERT INTO `{name}` VALUES({vals})")


class SQLiteObject():
    """Universal wrapper for cached objects; defined by table schemas, the update/retrieve spec, and the re-cache trigger condition"""
 
    def __init__(self, sqlite_db, signature, table_schemas, trigger, update, retrieve):
        """Parse the update/retrieve spec and the re-cache trigger condition; create tables if they do not exist"""
        self.__sqlite_db, self.__table_schemas = sqlite_db, table_schemas
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
        with closing(connect(self.__sqlite_db)) as connection:
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
        with closing(connect(self.__sqlite_db)) as connection:
            logger_args = self.__identifier_field, self.__identifier_value
            try:
                connection.cursor().execute(delete_action)
                connection.cursor().execute(insert_action, values)
                msg = "Updated fields for SQLiteObject\n\t(%s == %s)"
                GeneFabLogger().info(msg, *logger_args)
            except OperationalError:
                msg = "Could not update fields for SQLiteObject\n\t(%s == %s)"
                GeneFabLogger().warning(msg, *logger_args)
                connection.rollback()
            else:
                connection.commit()
 
    def __make_table_part_name(self, table, i):
        return table if i == 0 else f"{table}://{i}"
 
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
        with closing(connect(self.__sqlite_db)) as connection:
            try:
                bounds = range(0, dataframe.shape[1], self.maxpartwidth)
                for i, bound in enumerate(bounds):
                    partname = self.__make_table_part_name(table, i)
                    GeneFabLogger().info(
                        "Creating table for SQLiteObject (%s == %s):\n\t%s",
                        self.__identifier_field, self.__identifier_value,
                        partname,
                    )
                    dataframe.iloc[:,bound:bound+self.maxpartwidth].to_sql(
                        partname, connection, index=True, if_exists="replace",
                        chunksize=1000, method=partial(mkinsert, name=partname),
                    )
            except (OperationalError, DatabaseError) as e:
                connection.rollback()
                msg = "Failed to insert SQLite table"
                _kw = dict(signature=self.__signature, error=str(e))
                raise GeneFabDatabaseException(msg, **_kw)
            else:
                GeneFabLogger().info(
                    "All tables inserted for SQLiteObject (%s == %s):\n\t%s",
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
        with closing(connect(self.__sqlite_db)) as connection:
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
        with closing(connect(self.__sqlite_db)) as connection:
            for i in count():
                partname = self.__make_table_part_name(table, i)
                query = f"SELECT * FROM `{partname}` LIMIT 0"
                try:
                    cursor = connection.cursor()
                    cursor.execute(query).fetchall()
                    desc = cursor.description
                    index_name = SQLiteIndexName(desc[0][0])
                    if index_name not in column_dispatcher:
                        column_dispatcher[index_name] = partname
                    for c in desc[1:]:
                        column_dispatcher[c[0]] = partname
                except OperationalError:
                    break
        if not column_dispatcher:
            msg = "No data found"
            raise GeneFabDatabaseException(msg, signature=self.__signature)
        else:
            return postprocess_function(OndemandSQLiteDataFrame_Single(
                self.__sqlite_db, column_dispatcher,
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
 
    @property
    def data(self):
        """Main interface: returns data associated with this SQLiteObject; will have auto-updated itself in the process if necessary"""
        if not is_singular_spec(self.__trigger_spec):
            msg = "SQLiteObject(): Only one 'trigger' field can be specified"
            raise GeneFabConfigurationException(msg, signature=self.__signature)
        else:
            table = validate_no_backtick(
                next(iter(self.__trigger_spec)), "table",
            )
            trigger_field = validate_no_backtick(
                next(iter(self.__trigger_spec[table])), "trigger_field",
            )
            trigger_function = self.__trigger_spec[table][trigger_field]
        with closing(connect(self.__sqlite_db)) as connection:
            query = f"""SELECT `{trigger_field}` FROM `{table}` WHERE
                `{self.__identifier_field}` == "{self.__identifier_value}" """
            ret = connection.cursor().execute(query).fetchall()
            if len(ret) == 0:
                trigger_value = None
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                trigger_value = validate_no_doublequote(
                    ret[0][0], "trigger_value",
                )
            else:
                m = "Conflicting trigger values for SQLiteObject\n\t(%s == %s)"
                logger_args = self.__identifier_field, self.__identifier_value
                GeneFabLogger().warning(m, *logger_args)
                self.__drop_self_from(connection, table)
                trigger_value = None
        if trigger_function(trigger_value):
            self.changed = True
            self.__update(trigger_field, trigger_value)
        else:
            self.changed = False
        return self.__retrieve()


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, data_getter, sqlite_db, table, identifier, timestamp, compressor, decompressor):
        if not table.startswith("BLOBS:"):
            msg = "Table name for SQLiteBlob must start with 'BLOBS:'"
            _kw = dict(table=table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
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
                    "blob": lambda: (compressor or as_is)(data_getter()),
                }],
            },
            retrieve={table: {"blob": decompressor or as_is}},
        )


class SQLiteTable(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a generic table"""
 
    def __init__(self, data_getter, sqlite_db, table, aux_table, identifier, timestamp, maxpartwidth=1000):
        if not table.startswith("TABLE:"):
            msg = "Table name for SQLiteTable must start with 'TABLE:'"
            _kw = dict(table=table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
        if not aux_table.startswith("AUX:"):
            msg = "Aux table name for SQLiteTable must start with 'AUX:'"
            _kw = dict(aux_table=aux_table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
        self.maxpartwidth = maxpartwidth
        SQLiteObject.__init__(
            self, sqlite_db, signature={"identifier": identifier},
            table_schemas={
                table: None,
                aux_table: {"identifier": "TEXT", "timestamp": "INTEGER"},
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
                }]),
            )),
            retrieve={table: as_is},
        )
