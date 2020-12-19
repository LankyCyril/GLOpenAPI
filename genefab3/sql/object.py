from genefab3.common.utils import iterate_terminal_leaves
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.common.types import ImmutableTree
from genefab3.common.types_legacy import PlaceholderLogger
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError


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
 
    def __update(self, trigger_field, trigger_value):
        """Update the table (or blob) in SQLite and rewrite `trigger_field` with the incoming value, if in table"""
        for table, specs in self.__update_spec.items():
            for spec in specs:
                fields, values = sorted(spec), []
                delete_action = f"""
                    DELETE FROM '{table}'
                    WHERE {trigger_field} = '{trigger_value}'
                    AND {self.__identifier_field} = '{self.__identifier_value}'
                """
                insert_action = f"""
                    INSERT INTO '{table}' ({", ".join(fields)})
                    VALUES ({", ".join("?" for _ in fields)})
                """
                for field in fields:
                    value = spec[field]()
                    if self.__table_schemas[table][field] == "BLOB":
                        values.append(Binary(bytes(value)))
                    else:
                        values.append(value)
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
 
    def __retrieve(self):
        """Retrieve target table (or blob) from database"""
        if not is_singular_spec(self.__retrieve_spec):
            # TODO: interpret target as being a table instead
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'retrieve' field can be specified",
                signature=self.__signature,
            )
        else:
            table = next(iter(self.__retrieve_spec))
            field = next(iter(self.__retrieve_spec[table]))
            postprocess_function = self.__retrieve_spec[table][field]
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
