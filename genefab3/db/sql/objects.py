from genefab3.common.utils import iterate_terminal_leaves, as_is
from genefab3.common.exceptions import GeneFabConfigurationException
from copy import deepcopy
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from genefab3.common.logger import GeneFabLogger
from pandas import DataFrame
from collections.abc import Callable
from collections import OrderedDict
from genefab3.common.exceptions import GeneFabDatabaseException
from pandas.io.sql import DatabaseError
from itertools import count


def is_singular_spec(spec):
    """Check if a dictionary passed to SQLiteObject() has only one entry"""
    try:
        if not isinstance(spec, dict):
            return False
        elif sum(1 for _ in iterate_terminal_leaves(spec)) != 1:
            return False
        else:
            return True
    except ValueError:
        return False


class SQLiteObject():
    """Universal wrapper for cached objects; defined by table schemas, the update/retrieve spec, and the re-cache trigger condition"""
 
    def __init__(self, sqlite_db, signature, table_schemas, trigger, update, retrieve):
        """Parse the update/retrieve spec and the re-cache trigger condition; create tables if they do not exist"""
        self.__sqlite_db, self.__table_schemas = sqlite_db, table_schemas
        if len(signature) != 1:
            msg = "SQLiteObject(): Only one 'identifier' field can be specified"
            raise GeneFabConfigurationException(msg, signatures=signature)
        else:
            self.__identifier_field, self.__identifier_value = next(
                iter(signature.items()),
            )
            try:
                self.__signature = deepcopy(signature)
            except ValueError:
                msg = "SQLiteObject(): Bad signature"
                raise GeneFabConfigurationException(msg, signature=signature)
        if sqlite_db is not None: # TODO: auto_vacuum
            for table, schema in table_schemas.items():
                if schema is not None:
                    self.__ensure_table(table, schema)
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
                "CREATE TABLE IF NOT EXISTS '{}' ({})".format(
                    table, ", ".join(f"'{f}' {k}" for f, k in schema.items()),
                ),
            )
            connection.commit()
 
    def __update_fields(self, table, spec, trigger_field, trigger_value):
        """Update table field(s) in SQLite and drop `trigger_field` (which should be replaced according to spec)"""
        fields, values = sorted(spec), []
        for field in fields:
            value = spec[field]()
            if self.__table_schemas[table][field] == "BLOB":
                values.append(Binary(bytes(value)))
            else:
                values.append(value)
        delete_action = f"""
            DELETE FROM '{table}' WHERE {trigger_field} = '{trigger_value}'
            AND {self.__identifier_field} = '{self.__identifier_value}'
        """
        insert_action = f"""
            INSERT INTO '{table}' ({", ".join(fields)})
            VALUES ({", ".join("?" for _ in fields)})
        """
        with closing(connect(self.__sqlite_db)) as connection:
            try:
                connection.cursor().execute(delete_action)
                connection.cursor().execute(insert_action, values)
                GeneFabLogger().info(
                    "Updated SQLiteObject (%s == %s) fields",
                    self.__identifier_field, self.__identifier_value,
                )
            except OperationalError:
                GeneFabLogger().warning(
                    "Could not update SQLiteObject (%s == %s) fields",
                    self.__identifier_field, self.__identifier_value,
                )
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
                        "Creating table for SQLiteObject (%s == %s): %s",
                        self.__identifier_field, self.__identifier_value,
                        partname,
                    )
                    dataframe.iloc[:,bound:bound+self.maxpartwidth].to_sql(
                        partname, connection, index=True, if_exists="replace",
                    )
            except (OperationalError, DatabaseError):
                msg = "Failed to insert SQLite table"
                connection.rollback()
                raise GeneFabDatabaseException(msg, signature=self.__signature)
            else:
                GeneFabLogger().info(
                    "All table parts inserted for SQLiteObject (%s == %s): %s",
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
            connection.cursor().execute(f"""
                DELETE FROM '{table}' WHERE
                {self.__identifier_field} = '{self.__identifier_value}'
            """)
        except OperationalError:
            GeneFabLogger().warning(
                "Could not drop multiple entries for same %s == %s",
                self.__identifier_field, self.__identifier_value,
            )
 
    def __retrieve_field(self, table, field, postprocess_function):
        """Retrieve target table field from database"""
        with closing(connect(self.__sqlite_db)) as connection:
            query = f"""
                SELECT {field} from '{table}' WHERE
                {self.__identifier_field} = '{self.__identifier_value}'
            """
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
        """Retrieve target table from database; join if multiple exist""" # TODO now phantom
        from genefab3.db.sql.types import SQLiteIndexName
        column_dispatcher = OrderedDict()
        with closing(connect(self.__sqlite_db)) as connection:
            for i in count():
                partname = self.__make_table_part_name(table, i)
                query = f"SELECT * FROM '{partname}' LIMIT 0"
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
            from genefab3.db.sql.types import OndemandSQLiteDataFrame
            return postprocess_function(
                OndemandSQLiteDataFrame(self.__sqlite_db, column_dispatcher),
            )
        # TODO: here's where the magic will happen, but for now just join all
 
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
            table = next(iter(self.__trigger_spec))
            trigger_field = next(iter(self.__trigger_spec[table]))
            trigger_function = self.__trigger_spec[table][trigger_field]
        with closing(connect(self.__sqlite_db)) as connection:
            query = f"""
                SELECT {trigger_field} FROM '{table}'
                WHERE {self.__identifier_field} = '{self.__identifier_value}'
            """
            ret = connection.cursor().execute(query).fetchall()
            if len(ret) == 0:
                trigger_value = None
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                trigger_value = ret[0][0]
            else:
                GeneFabLogger().warning(
                    "Conflicting trigger values for SQLiteObject (%s == %s)",
                    self.__identifier_field, self.__identifier_value,
                )
                self.__drop_self_from(connection, table)
                trigger_value = None
        if trigger_function(trigger_value):
            self.changed = True
            self.__update(trigger_field, trigger_value)
        else:
            self.changed = False
        return self.__retrieve()
