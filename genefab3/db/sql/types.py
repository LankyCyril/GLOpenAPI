from genefab3.common.utils import iterate_terminal_leaves, as_is
from genefab3.common.exceptions import GeneFabConfigurationException
from copy import deepcopy
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from genefab3.common.logger import GeneFabLogger
from pandas import read_sql, DataFrame, read_csv
from genefab3.common.exceptions import GeneFabDatabaseException
from pandas.io.sql import DatabaseError
from urllib.request import urlopen
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from tempfile import TemporaryDirectory
from os import path
from shutil import copyfileobj
from csv import Error as CSVError, Sniffer
from pandas.errors import ParserError as PandasParserError
from genefab3.common.exceptions import GeneFabFileException


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
            except OperationalError:
                GeneFabLogger().warning(
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
            msg = "Cached table not represented as a pandas DataFrame"
            raise NotImplementedError(msg)
        if dataframe.index.nlevels != 1:
            raise NotImplementedError("MultiIndex in cached DataFrame")
        if dataframe.columns.nlevels != 1:
            raise NotImplementedError("MultiIndex columns in cached DataFrame")
        with closing(connect(self.__sqlite_db)) as connection:
            dataframe.to_sql(table, connection, index=True, if_exists="replace")
 
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
 
    def __retrieve_table(self, table, postprocess_function):
        """Retrieve target table from database"""
        with closing(connect(self.__sqlite_db)) as connection:
            query = f"SELECT * from '{table}'"
            try:
                dataframe = read_sql(query, connection)
                dataframe.set_index(dataframe.columns[0], inplace=True)
                return postprocess_function(dataframe)
            except (OperationalError, DatabaseError):
                msg = "No data found"
                raise GeneFabDatabaseException(msg, signature=self.__signature)
 
    def __retrieve(self):
        """Retrieve target table or table field from database"""
        if not is_singular_spec(self.__retrieve_spec):
            msg = "SQLiteObject(): Only one 'retrieve' field can be specified"
            raise GeneFabConfigurationException(msg, signature=self.__signature)
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


class SQLiteBlob(SQLiteObject):
    """Represents an SQLiteObject initialized with a spec suitable for a binary blob"""
 
    def __init__(self, data_getter, sqlite_db, table, identifier, timestamp, compressor, decompressor):
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
 
    def __init__(self, data_getter, sqlite_db, table, aux_table, identifier, timestamp):
        if table == aux_table:
            msg = "Table name cannot be equal to a reserved table name"
            _kw = dict(table=table, identifier=identifier)
            raise GeneFabConfigurationException(msg, **_kw)
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
            update={
                table: [data_getter],
                aux_table: [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                }],
            },
            retrieve={table: as_is},
        )


class CachedBinaryFile(SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="BLOBS:blobs", compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.aux_table = aux_table
        SQLiteBlob.__init__(
            self, identifier=identifier, timestamp=timestamp,
            data_getter=lambda: self.__download_as_blob(urls),
            sqlite_db=sqlite_db,
            table=aux_table,
            compressor=compressor, decompressor=decompressor,
        )
 
    def __download_as_blob(self, urls):
        """Download data from URL as-is"""
        self.url, data = None, None
        for url in urls:
            try:
                with urlopen(url) as response:
                    data = response.read()
            except URLError:
                msg = f"{self.name}; tried URL and failed: {url}"
                GeneFabLogger().warning(msg)
            else:
                msg = f"{self.name}; successfully fetched blob: {url}"
                GeneFabLogger().info(msg)
                self.url = url
                return data
        else:
            msg = "None of the URLs are reachable for file"
            raise GeneFabDataManagerException(msg, name=self.name, urls=urls)


class CachedTableFile(SQLiteTable):
    """Represents an SQLiteObject that stores up-to-date file contents as generic table"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="AUX:timestamp_table", INPLACE_process=as_is, **pandas_kws):
        """Interpret file descriptors; inherit functionality from SQLiteTable; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.aux_table = aux_table
        SQLiteTable.__init__(
            self, identifier=f"TABLE:{identifier}", timestamp=timestamp,
            data_getter=lambda: self.__download_as_pandas_dataframe(
                urls, pandas_kws, INPLACE_process,
            ),
            sqlite_db=sqlite_db,
            table=f"TABLE:{identifier}", aux_table=aux_table,
        )
 
    def __copyfileobj(self, urls, tempfile):
        """Try all URLs and push data into temporary file"""
        for url in urls:
            with open(tempfile, mode="wb") as handle:
                try:
                    with urlopen(url) as response:
                        copyfileobj(response, handle)
                except URLError:
                    msg = f"{self.name}; tried URL and failed: {url}"
                    GeneFabLogger().warning(msg)
                else:
                    msg = f"{self.name}; successfully fetched data: {url}"
                    GeneFabLogger().info(msg)
                    return url
        else:
            msg = "None of the URLs are reachable for file"
            raise GeneFabDataManagerException(msg, name=self.name, urls=urls)
 
    def __download_as_pandas_dataframe(self, urls, pandas_kws, INPLACE_process=as_is):
        """Download and parse data from URL as a table"""
        with TemporaryDirectory() as tempdir:
            tempfile = path.join(tempdir, self.name)
            self.url = self.__copyfileobj(urls, tempfile)
            with open(tempfile, mode="rb") as handle:
                magic = handle.read(3)
            if magic == b"\x1f\x8b\x08":
                compression = "gzip"
                from gzip import open as _open
            elif magic == b"\x42\x5a\x68":
                compression = "bz2"
                from bz2 import open as _open
            else:
                compression, _open = "infer", open
            try:
                with _open(tempfile, mode="rt", newline="") as handle:
                    sep = Sniffer().sniff(handle.read(2**20)).delimiter
                dataframe = read_csv(
                    tempfile, sep=sep, compression=compression, **pandas_kws,
                )
                msg = f"{self.name}; interpreted as table: {tempfile}"
                GeneFabLogger().info(msg)
                INPLACE_process(dataframe)
                return dataframe
            except (IOError, UnicodeDecodeError, CSVError, PandasParserError):
                msg = "Not recognized as a table file"
                raise GeneFabFileException(msg, name=self.name, url=self.url)
