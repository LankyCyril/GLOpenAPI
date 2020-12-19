from genefab3.config import TIMESTAMP_FMT
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.common.exceptions import GeneLabJSONException
from genefab3.common.exceptions import GeneLabDataManagerException
from collections.abc import Hashable
from datetime import datetime
from itertools import zip_longest
from numpy import nan
from werkzeug.datastructures import ImmutableDict
from urllib.request import urlopen
from contextlib import closing
from sqlite3 import connect, Binary


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


class SQLiteObject():
    """..."""
 
    def __init__(self, sqlite_db, identifier, table_schemas, trigger, update, retrieve):
        # TODO: auto_vacuum
        self.__sqlite_db, self.__table_schemas = sqlite_db, table_schemas
        self.__identifier_field, self.__identifier_value = identifier
        if sqlite_db is not None:
            for table, schema in table_schemas.items():
                self.__ensure_table(table, schema)
        self.__trigger_spec = ImmutableTree(trigger)
        self.__retrieve_spec = ImmutableTree(retrieve)
        self.__update_spec = ImmutableTree(update)
 
    def __ensure_table(self, table, schema):
        with closing(connect(self.__sqlite_db)) as connection:
            connection.cursor().execute(
                "CREATE TABLE IF NOT EXISTS '{}' ({})".format(
                    table, ", ".join(f"'{f}' {k}" for f, k in schema.items())
                )
            )
            connection.commit()
 
    def __update(self, trigger_field, trigger_value):
        for table, spec in self.__update_spec.items():
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
                connection.cursor().execute(delete_action)
                connection.cursor().execute(insert_action, values)
                # TODO except operational error -- rollback all, warn stale
                connection.commit()
 
    def __retrieve(self):
        if sum(1 for _ in iterate_terminal_leaves(self.__retrieve_spec)) != 1:
            raise GeneLabDatabaseException(
                "Only a single 'retrieve' field can be specified",
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
                    "No data found", table=table,
                    **{self.__identifier_field: self.__identifier_value},
                )
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                return postprocess_function(ret[0][0])
            else:
                raise GeneLabDatabaseException( # TODO: drop and raise
                    "Multiple entries in database", table=table,
                    **{self.__identifier_field: self.__identifier_value},
                )
 
    @property
    def data(self):
        if sum(1 for _ in iterate_terminal_leaves(self.__trigger_spec)) != 1:
            raise GeneLabDatabaseException(
                "Only a single 'trigger' field can be specified",
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
                trigger_value = nan
            elif (len(ret) == 1) and (len(ret[0]) == 1):
                trigger_value = ret[0][0]
            else:
                raise GeneLabDatabaseException( # TODO: drop and set to nan
                    "Multiple entries in database", table=table,
                    field=self.__identifier_field,
                    value=self.__identifier_value,
                )
        if trigger_function(trigger_value):
            self.__update(trigger_field, trigger_value)
        return self.__retrieve()


class SQLiteBlob(SQLiteObject):
    """..."""
 
    def __init__(self, sqlite_db, table, identifier, timestamp, url, compressor, decompressor):
        SQLiteObject.__init__(
            self, sqlite_db, identifier=("identifier", identifier),
            table_schemas={
                table: {
                    "identifier": "TEXT",
                    "timestamp": "INTEGER",
                    "blob": "BLOB",
                },
            },
            trigger={
                table: {
                    "timestamp": lambda value: not (timestamp <= value)
                },
            },
            update={
                table: {
                    "identifier": lambda: identifier,
                    "timestamp": lambda: timestamp,
                    "blob": lambda: self.__download(url, compressor),
                },
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
