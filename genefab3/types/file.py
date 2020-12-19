from genefab3.config import TIMESTAMP_FMT
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.common.exceptions import GeneLabJSONException
from genefab3.common.exceptions import GeneLabDataManagerException
from collections.abc import Hashable
from genefab3.common.types import PlaceholderLogger
from datetime import datetime
from itertools import zip_longest
from numpy import nan
from werkzeug.datastructures import ImmutableDict
from urllib.request import urlopen
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError


noop = lambda _:_


def iterate_terminal_leaves(d, step_tracker=1, max_steps=256):
    """Descend into branches breadth-first and iterate terminal leaves"""
    if step_tracker >= max_steps:
        raise ValueError(
            "Dictionary exceeded nestedness threshold", max_steps,
        )
    else:
        if isinstance(d, dict):
            for i, branch in enumerate(d.values(), start=1):
                yield from iterate_terminal_leaves(branch, step_tracker+i)
        else:
            yield d


def ImmutableTree(d, step_tracker=1, max_steps=256):
    """..."""
    if step_tracker >= max_steps:
        raise ValueError("Tree exceeded nestedness threshold", max_steps)
    elif isinstance(d, dict):
        return ImmutableDict({
            k: ImmutableTree(v, step_tracker+i)
            for i, (k, v) in enumerate(d.items(), start=1)
        })
    elif isinstance(d, (list, tuple)):
        return tuple(
            ImmutableTree(v, step_tracker+i)
            for i, v in enumerate(d, start=1)
        )
    else:
        return d


class HashableEnough():
    """..."""

    def __init__(self, identity_fields, as_strings=()):
        self.__identity_fields = tuple(identity_fields)
        self.__as_strings = set(as_strings)

    def __iter_identity_values__(self):
        """Iterate values of identity fields in a hash-friendly manner"""
        for field in self.__identity_fields:
            value = getattr(self, field, nan)
            if field in self.__as_strings:
                value = str(value)
            if not isinstance(value, Hashable):
                raise TypeError(
                    "{}: unhashable field value".format(type(self).__name__),
                    f"{field}={repr(value)}",
                )
            else:
                yield value

    def __eq__(self, other):
        """..."""
        return all(s == o for s, o in zip_longest(
            self.__iter_identity_values__(),
            getattr(other, "__iter_identity_values__", lambda: ())(),
            fillvalue=nan,
        ))

    def __hash__(self):
        """..."""
        return hash(tuple(self.__iter_identity_values__()))


def is_singular_spec(spec):
    if not isinstance(spec, dict):
        return False
    elif sum(1 for _ in iterate_terminal_leaves(spec)) != 1:
        return False
    else:
        return True


class SQLiteObject():
    """..."""
 
    def __init__(self, sqlite_db, identifier_dict, table_schemas, trigger, update, retrieve, logger=None):
        # TODO: auto_vacuum
        self.__sqlite_db, self.__table_schemas = sqlite_db, table_schemas
        if len(identifier_dict) != 1:
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'identifier' field can be specified",
                identifier=identifier_dict,
            )
        else:
            self.__identifier_field, self.__identifier_value = next(
                iter(identifier_dict.items()),
            )
            self.__identifier_dict = ImmutableTree(identifier_dict)
        if sqlite_db is not None:
            for table, schema in table_schemas.items():
                self.__ensure_table(table, schema)
        self.__trigger_spec = ImmutableTree(trigger)
        self.__retrieve_spec = ImmutableTree(retrieve)
        self.__update_spec = ImmutableTree(update)
        self.__logger = logger or PlaceholderLogger()
 
    def __ensure_table(self, table, schema):
        with closing(connect(self.__sqlite_db)) as connection:
            connection.cursor().execute(
                "CREATE TABLE IF NOT EXISTS '{}' ({})".format(
                    table, ", ".join(f"'{f}' {k}" for f, k in schema.items())
                )
            )
            connection.commit()
 
    def __update(self, trigger_field, trigger_value):
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
        if not is_singular_spec(self.__retrieve_spec):
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'retrieve' field can be specified",
                identifier=self.__identifier_dict,
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
                    "No data found", identifier=self.__identifier_dict,
                )
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return postprocess_function(ret[0][0])
            else:
                self.__drop_self_from(connection, table)
                raise GeneLabDatabaseException(
                    "Entries conflict (will attempt to fix on next request)",
                    identifier=self.__identifier_dict,
                )
 
    @property
    def data(self):
        if not is_singular_spec(self.__trigger_spec):
            raise GeneLabDatabaseException(
                "SQLiteObject(): Only one 'trigger' field can be specified",
                identifier=self.__identifier_dict,
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


class SQLiteBlob(SQLiteObject):
    """..."""
 
    def __init__(self, sqlite_db, table, identifier, timestamp, url, compressor, decompressor):
        SQLiteObject.__init__(
            self, sqlite_db, {"identifier": identifier},
            table_schemas={
                table: {
                    "identifier": "TEXT",
                    "timestamp": "INTEGER",
                    "blob": "BLOB",
                },
            },
            trigger={
                table: {
                    "timestamp": lambda value:
                        (value is None) or (timestamp > value)
                },
            },
            update={
                table: [{
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                    "blob": lambda: self.__download(url, compressor),
                }],
            },
            retrieve={
                table: {
                    "blob": noop if decompressor is None else decompressor,
                },
            },
        )
 
    def __download(self, url, compressor):
        with urlopen(url) as response:
            return (compressor or noop)(response.read())


def extract_timestamp(json, key="date_modified", fallback_key="date_created", fallback_value=-1, fmt=TIMESTAMP_FMT):
    """Convert date like 'Fri Oct 11 22:02:48 EDT 2019' to timestamp"""
    try:
        dt = datetime.strptime(json.get(key, json.get(fallback_key)), fmt)
    except (ValueError, TypeError):
        return fallback_value
    else:
        return int(dt.timestamp())


FILE_JSON_ERROR = "CacheableFile: missing key in file JSON"


class CacheableFile(HashableEnough, SQLiteBlob):
    """..."""
    __identity_fields = "name", "url", "timestamp", "sqlite_db"
 
    def __ingest_arguments__(self, *, json, name, url, timestamp, json_timestamp_extractor):
        """Initialize either from `json` or from `name, url, timestamp`"""
        if json is not None:
            try:
                self.name, self.url = json["name"], json["remote_url"]
                if json_timestamp_extractor is None:
                    self.timestamp = None
                elif isinstance(json_timestamp_extractor, str):
                    self.timestamp = json[json_timestamp_extractor]
                else:
                    self.timestamp = json_timestamp_extractor(json)
            except KeyError as e:
                raise GeneLabJSONException(FILE_JSON_ERROR, key=str(e))
        if name is not None:
            self.name = name
        if url is not None:
            self.url = url
        if timestamp is not None:
            self.timestamp = timestamp
        if (not hasattr(self, "url")) or (self.url is None):
            raise GeneLabDataManagerException("No URL for file", name=self.name)
 
    def __init__(self, *, json=None, name=None, url=None, timestamp=None, sqlite_db=None, json_timestamp_extractor=None, compressor=None, decompressor=None):
        """..."""
        self.__ingest_arguments__(
            json=json, name=name, url=url, timestamp=timestamp,
            json_timestamp_extractor=json_timestamp_extractor,
        )
        SQLiteBlob.__init__(
            self, sqlite_db=sqlite_db, table="blobs",
            identifier=self.url, timestamp=self.timestamp,
            url=self.url, compressor=compressor, decompressor=decompressor,
        )
        HashableEnough.__init__(
            self, self.__identity_fields,
        )
