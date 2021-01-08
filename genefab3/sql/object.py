from genefab3.common.utils import iterate_terminal_leaves
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.common.types import ImmutableTree, PlaceholderLogger
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from pandas import read_sql, DataFrame
from pandas.io.sql import DatabaseError


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
 
    def __init__(self, sqlite_db, signature, table_schemas, trigger, update, retrieve, logger=None):
        """Parse the update/retrieve spec and the re-cache trigger condition; create tables if they do not exist"""
        self.__sqlite_db, self.__table_schemas = sqlite_db, table_schemas
        if len(signature) != 1:
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'identifier' field can be specified",
                signatures=signature,
            )
        else:
            self.__identifier_field, self.__identifier_value = next(
                iter(signature.items()),
            )
            try:
                self.__signature = ImmutableTree(signature)
            except ValueError:
                raise GeneLabDatabaseException(
                    "SQLiteObject(): Bad signature", signature=signature,
                )
        if sqlite_db is not None: # TODO: auto_vacuum
            for table, schema in table_schemas.items():
                if schema is not None:
                    self.__ensure_table(table, schema)
        try:
            self.__trigger_spec = ImmutableTree(trigger)
            self.__retrieve_spec = ImmutableTree(retrieve)
            self.__update_spec = ImmutableTree(update)
        except ValueError:
            raise GeneLabDatabaseException(
                "SQLiteObject(): Bad spec", trigger=trigger,
                update=update, retrieve=retrieve,
            )
        self.__logger = logger or PlaceholderLogger()
 
    def __ensure_table(self, table, schema):
        """Create table with schema, provided as a dictionary"""
        with closing(connect(self.__sqlite_db)) as connection:
            connection.cursor().execute(
                "CREATE TABLE IF NOT EXISTS '{}' ({})".format(
                    table, ", ".join(f"'{f}' {k}" for f, k in schema.items())
                )
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
            DELETE FROM '{table}'
            WHERE {trigger_field} = '{trigger_value}'
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
            except OperationalError:
                self.__logger.warning(
                    "Could not update SQLiteObject (%s == %s)",
                    self.__identifier_field, self.__identifier_value,
                )
                connection.rollback()
            else:
                connection.commit()
 
    def __update_table(self, table, spec):
        """Update table in SQLite"""
        dataframe = spec()
        if not isinstance(dataframe, DataFrame):
            raise NotImplementedError(
                "Cached table not represented as a pandas DataFrame",
            )
        elif dataframe.columns.nlevels != 1:
            raise NotImplementedError(
                "Cached DataFrame with MultiIndex columns",
            )
        elif dataframe.index.nlevels != 1:
            raise NotImplementedError(
                "Cached DataFrame with MultiIndex index",
            )
        elif dataframe.index.name not in {None, "index"}:
            raise NotImplementedError(
                "Cached DataFrame index name neither 'index' nor None",
            )
        else:
            with closing(connect(self.__sqlite_db)) as connection:
                dataframe.to_sql(
                    table, connection, index=True,
                    if_exists="replace",
                )
 
    def __update(self, trigger_field, trigger_value):
        """Update table or table field in SQLite and drop `trigger_field` (to be replaced according to spec)"""
        for table, specs in self.__update_spec.items():
            for spec in specs:
                if isinstance(spec, dict):
                    self.__update_fields(
                        table, spec, trigger_field, trigger_value,
                    )
                else:
                    self.__update_table(table, spec)
 
    def __drop_self_from(self, connection, table):
        """Helper method (during an open connection) to drop rows matching `self.signature` from `table`"""
        try:
            connection.cursor.execute(f"""
                DELETE FROM '{table}' WHERE
                {self.__identifier_field} = '{self.__identifier_value}'
            """)
        except OperationalError:
            self.__logger.warning(
                "Could not drop multiple entries for same %s == %s",
                self.__identifier_field, self.__identifier_value,
            )
 
    def __retrieve_field(self, table, field, postprocess_function):
        """Retrieve target table field from database"""
        with closing(connect(self.__sqlite_db)) as connection:
            query = f"""
                SELECT {field} from '{table}'
                WHERE {self.__identifier_field} = '{self.__identifier_value}'
            """
            ret = connection.cursor().execute(query).fetchall()
            if len(ret) == 0:
                raise GeneLabDatabaseException(
                    "No data found", signature=self.__signature,
                )
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return postprocess_function(ret[0][0])
            else:
                self.__drop_self_from(connection, table)
                raise GeneLabDatabaseException(
                    "Entries conflict (will attempt to fix on next request)",
                    signature=self.__signature,
                )
 
    def __retrieve_table(self, table, postprocess_function):
        """Retrieve target table from database"""
        with closing(connect(self.__sqlite_db)) as connection:
            query = f"SELECT * from '{table}'"
            try:
                return postprocess_function(
                    read_sql(query, connection, index_col="index"),
                )
            except (OperationalError, DatabaseError):
                raise GeneLabDatabaseException(
                    "No data found", signature=self.__signature,
                )
 
    def __retrieve(self):
        """Retrieve target table or table field from database"""
        if not is_singular_spec(self.__retrieve_spec):
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'retrieve' field can be specified",
                signature=self.__signature,
            )
        else:
            table = next(iter(self.__retrieve_spec))
            if isinstance(self.__retrieve_spec[table], dict):
                field = next(iter(self.__retrieve_spec[table]))
                postprocess_function = self.__retrieve_spec[table][field]
                return self.__retrieve_field(table, field, postprocess_function)
            else:
                postprocess_function = self.__retrieve_spec[table]
                return self.__retrieve_table(table, postprocess_function)
 
    @property
    def data(self):
        """Main interface: returns data associated with this SQLiteObject; will have auto-updated itself in the process if necessary"""
        if not is_singular_spec(self.__trigger_spec):
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'trigger' field can be specified",
                signature=self.__signature,
            )
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
                self.__logger.warning(
                    "Conflicting trigger values for SQLiteObject (%s == %s)",
                    self.__identifier_field, self.__identifier_value,
                )
                self.__drop_self_from(connection, table)
                trigger_value = None
        if trigger_function(trigger_value):
            self.__update(trigger_field, trigger_value)
        return self.__retrieve()
