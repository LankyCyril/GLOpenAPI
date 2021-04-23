from genefab3.db.sql.objects import SQLiteObject, validate_no_backtick
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.utils import as_is
from collections import OrderedDict
from genefab3.common.logger import GeneFabLogger
from urllib.request import urlopen
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabDataManagerException
from shutil import copyfileobj
from tempfile import TemporaryDirectory
from os import path
from csv import Error as CSVError, Sniffer
from pandas import read_csv, read_sql, Index, MultiIndex
from pandas.errors import ParserError as PandasParserError
from genefab3.common.exceptions import GeneFabFileException
from genefab3.common.exceptions import GeneFabDatabaseException
from contextlib import contextmanager, closing, ExitStack
from uuid import uuid3, uuid4
from sqlite3 import OperationalError, connect
from genefab3.api.renderers import Placeholders
from genefab3.common.types import DataDataFrame
from pandas.io.sql import DatabaseError as PandasDatabaseError


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
 

class CachedBinaryFile(SQLiteBlob):
    """Represents an SQLiteObject that stores up-to-date file contents as a binary blob"""
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, table="BLOBS:blobs", compressor=None, decompressor=None):
        """Interpret file descriptors; inherit functionality from SQLiteBlob; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.table = table
        SQLiteBlob.__init__(
            self, identifier=identifier, timestamp=timestamp,
            data_getter=lambda: self.__download_as_blob(urls),
            sqlite_db=sqlite_db, table=table,
            compressor=compressor, decompressor=decompressor,
        )
 
    def __download_as_blob(self, urls):
        """Download data from URL as-is"""
        self.url, data = None, None
        for url in urls:
            msg = f"{self.name}; trying URL: {url}"
            GeneFabLogger().info(msg)
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
 
    def __init__(self, *, name, identifier, urls, timestamp, sqlite_db, aux_table="AUX:timestamp_table", INPLACE_process=as_is, maxpartwidth=1000, **pandas_kws):
        """Interpret file descriptors; inherit functionality from SQLiteTable; define equality (hashableness) of self"""
        self.name, self.url, self.timestamp = name, None, timestamp
        self.identifier = identifier
        self.aux_table = aux_table
        self.table = f"TABLE:{identifier}"
        SQLiteTable.__init__(
            self, identifier=f"TABLE:{identifier}", timestamp=timestamp,
            data_getter=lambda: self.__download_as_pandas_dataframe(
                urls, pandas_kws, INPLACE_process,
            ),
            sqlite_db=sqlite_db, maxpartwidth=maxpartwidth,
            table=self.table, aux_table=aux_table,
        )
 
    def __copyfileobj(self, urls, tempfile):
        """Try all URLs and push data into temporary file"""
        for url in urls:
            with open(tempfile, mode="wb") as handle:
                msg = f"{self.name}; trying URL: {url}"
                GeneFabLogger().info(msg)
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


class SQLiteIndexName(str): pass


class OndemandSQLiteDataFrame():
 
    @property
    def columns(self): return self._columns
    @columns.setter
    def columns(self, value):
        if isinstance(value, Index):
            all_levels, last_level = value, value.get_level_values(-1)
        else:
            all_levels, last_level = Index(value), value
        if set(last_level) <= set(self._raw_columns):
            self._raw_columns, self._columns = list(last_level), all_levels
        else:
            msg = f"Setting foreign column(s) to OndemandSQLiteDataFrame"
            foreign = sorted(set(last_level) - set(self._raw_columns))
            raise GeneFabFileException(msg, columns=foreign)
 
    @staticmethod
    def concat(objs, axis=1):
        """Concatenate OndemandSQLiteDataFrame objects without evaluation"""
        _t, _t_s = "OndemandSQLiteDataFrame", "OndemandSQLiteDataFrame_Single"
        _Single = OndemandSQLiteDataFrame_Single
        if len(objs) == 1:
            return objs[0]
        elif not all(isinstance(o, _Single) for o in objs):
            raise TypeError(f"{_t}.concat() on non-{_t_s} objects")
        elif axis != 1:
            raise ValueError(f"{_t}.concat(..., axis={axis}) makes no sense")
        elif len(set(obj.sqlite_db for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects from different database files"
            raise ValueError(msg)
        else:
            sqlite_db = objs[0].sqlite_db
            return OndemandSQLiteDataFrame_OuterJoined(sqlite_db, objs)


def _make_query_filter(table, rows, limit, offset):
    """Validate arguments to OndemandSQLiteDataFrame_Single.get()"""
    if (offset != 0) and (limit is None):
        msg = "OndemandSQLiteDataFrame: `offset` without `limit`"
        raise GeneFabDatabaseException(msg, table=table)
    if rows is not None:
        raise NotImplementedError("Slicing by row names")
    if limit is None:
        return ""
    else:
        return f"LIMIT {limit} OFFSET {offset}"


@contextmanager
def _make_view(connection, query):
    """Context manager temporarily creating an SQLite view from `query`"""
    viewname = uuid3(uuid4(), query).hex
    try:
        connection.cursor().execute(f"CREATE VIEW `{viewname}` as {query}")
    except OperationalError:
        msg = "Failed to create temporary view"
        _kw = dict(viewname=viewname, query=query)
        raise GeneFabDatabaseException(msg, **_kw)
    else:
        query_repr = repr(query.lstrip()[:100] + "...")
        msg = f"Created temporary SQLite view {viewname} from {query_repr}"
        GeneFabLogger().info(msg)
        yield viewname
    try:
        connection.cursor().execute(f"DROP VIEW `{viewname}`")
    except OperationalError:
        msg = f"Failed to drop temporary view {viewname}"
        GeneFabLogger().error(msg)
    else:
        msg = f"Dropped temporary SQLite view {viewname}"
        GeneFabLogger().info(msg)


class OndemandSQLiteDataFrame_Single(OndemandSQLiteDataFrame):
    """DataDataFrame to be retrieved from SQLite, possibly from multiple parts of same tabular file"""
 
    def __init__(self, sqlite_db, column_dispatcher):
        """Interpret `column_dispatcher`"""
        self.sqlite_db = sqlite_db
        self._column_dispatcher = column_dispatcher
        self.name = None
        self._raw_columns, _index_names = [], set()
        for n, p in column_dispatcher.items():
            validate_no_backtick(n, "column")
            validate_no_backtick(p, "table_part")
            if isinstance(n, SQLiteIndexName):
                _index_names.add(n)
            else:
                self._raw_columns.append(n)
            if self.name is None:
                self.name = p
        if len(_index_names) == 0:
            msg = "OndemandSQLiteDataFrame(): no index"
            raise GeneFabDatabaseException(msg, table=self.name)
        elif len(_index_names) > 1:
            msg = "OndemandSQLiteDataFrame(): parts indexes do not match"
            _kw = dict(table=self.name, index_names=_index_names)
            raise GeneFabDatabaseException(msg, **_kw)
        self.index = Index([], name=_index_names.pop())
        self._columns = Index(self._raw_columns, name=None)
 
    def __make_inverse_column_dispatcher(self, columns):
        """Validate arguments and make `columns`, `part_to_column`"""
        if columns is not None:
            _cs = set(columns)
            if len(_cs) != len(columns):
                raise IndexError(f"Slicing by duplicate column name")
            elif self.index.name in _cs:
                raise IndexError(f"Requesting index as if it were a column")
        else:
            columns = self._raw_columns
        part_to_column = OrderedDict({
            # get index column from part containing first requested column:
            self._column_dispatcher[columns[0]]: [self.index.name],
        })
        for column in (self._raw_columns if columns is None else columns):
            part = self._column_dispatcher[column]
            part_to_column.setdefault(part, []).append(column)
        return columns, part_to_column
 
    def __make_natural_join_query(self, rows, part_to_column, columns, limit=None, offset=0):
        """Generate SQL query for multipart NATURAL JOIN"""
        query_filter = _make_query_filter(self.name, rows, limit, offset)
        joined = " NATURAL JOIN ".join(f"`{p}`" for p in part_to_column)
        first_table = next(iter(part_to_column))
        targets = ",".join((
            f"`{first_table}`.`{self.index.name}`",
            *(f"`{c}`" for c in columns),
        ))
        _n, _m = len(columns), len(part_to_column)
        _tt = "\n\t".join(("", *part_to_column))
        msg = f"retrieving {_n} columns from {_m} table(s):{_tt}"
        GeneFabLogger().info(f"{self.name}; {msg}")
        return f"SELECT {targets} FROM {joined} {query_filter}"
 
    def get(self, *, rows=None, columns=None, limit=None, offset=0):
        """Interpret arguments and retrieve data as DataDataFrame by running SQL queries"""
        columns, part_to_column = self.__make_inverse_column_dispatcher(columns)
        if len(part_to_column) == 0:
            return Placeholders.EmptyDataDataFrame()
        else:
            args = rows, part_to_column, columns, limit, offset
            query = self.__make_natural_join_query(*args)
            with closing(connect(self.sqlite_db)) as connection:
                try:
                    a, k = (query, connection), dict(index_col=self.index.name)
                    data = read_sql(*a, **k)
                except OperationalError:
                    msg = "No data found"
                    raise GeneFabDatabaseException(msg, table=self.name)
                else:
                    msg = f"retrieved from SQLite as pandas DataFrame"
                    GeneFabLogger().info(f"{self.name}; {msg}")
                    data.columns = self.columns
                    return DataDataFrame(data)
 
    @contextmanager
    def view(self, *, connection, rows=None, columns=None):
        """Interpret arguments and temporarily expose requested data as SQL view"""
        columns, part_to_column = self.__make_inverse_column_dispatcher(columns)
        if len(part_to_column) == 0:
            yield None
        else:
            args = rows, part_to_column, columns, None, 0
            join_query = self.__make_natural_join_query(*args)
            try:
                with _make_view(connection, join_query) as viewname:
                    yield viewname, columns
            except (OperationalError, GeneFabDatabaseException):
                raise GeneFabDatabaseException("No data found", table=self.name)


class OndemandSQLiteDataFrame_OuterJoined(OndemandSQLiteDataFrame):
    """DataDataFrame to be retrieved from SQLite, full outer joined from multiple views"""
            #return OndemandSQLiteDataFrame_OuterJoined(sqlite_db, columns)
 
    def __init__(self, sqlite_db, objs):
        _t = "OndemandSQLiteDataFrame"
        self.sqlite_db, self.objs = sqlite_db, objs
        if len(objs) < 2:
            raise ValueError(f"{type(self).__name__}: no objects to join")
        elif len(set(obj.index.name for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing index names"
            raise ValueError(msg)
        else:
            self.index = Index([], name=objs[0].index.name)
        if len(set(obj.columns.nlevels for obj in objs)) != 1:
            msg = f"Concatenating {_t} objects with differing column `nlevels`"
            raise ValueError(msg)
        else:
            if objs[0].columns.nlevels == 1:
                _mkindex = Index
            else:
                _mkindex = MultiIndex.from_tuples
            self._columns = _mkindex(
                sum((obj.columns.to_list() for obj in objs), []),
            )
            self._raw_columns = self._columns.get_level_values(-1)
            names = ", ".join(str(obj.name) for obj in objs)
            self.name = f"FullOuterJoin({names})"
 
    def get(self, *, rows=None, columns=None, limit=None, offset=0):
        """Interpret arguments and retrieve data as DataDataFrame by running SQL queries"""
        query_filter = _make_query_filter(self.name, rows, limit, offset)
        with closing(connect(self.sqlite_db)) as connection, ExitStack() as _st:
            _kw = dict(connection=connection, rows=rows, columns=columns)
            object_views = [_st.enter_context(o.view(**_kw)) for o in self.objs]
            left_view, left_columns = object_views[0]
            for right_view, right_columns in object_views[1:]:
                merged_columns = left_columns + right_columns
                targets = ",".join((
                    f"`{self.index.name}`", *(f"`{c}`" for c in merged_columns),
                ))
                condition = f"""`{left_view}`.`{self.index.name}` ==
                    `{right_view}`.`{self.index.name}`"""
                query = f"""SELECT `{left_view}`.{targets} FROM
                        `{left_view}` LEFT JOIN `{right_view}` ON {condition}
                    UNION SELECT `{right_view}`.{targets} FROM
                        `{right_view}` LEFT JOIN `{left_view}` ON {condition}
                        WHERE `{left_view}`.`{self.index.name}` IS NULL"""
                merged_view = _st.enter_context(_make_view(connection, query))
                left_view, left_columns = merged_view, merged_columns
            query = f"SELECT * FROM `{merged_view}` {query_filter}"
            try:
                data = read_sql(query, connection, index_col=self.index.name)
            except OperationalError:
                _st.close()
                raise GeneFabDatabaseException("No data found", table=self.name)
            except PandasDatabaseError:
                _st.close()
                msg = "Bad SQL query when joining tables"
                _kw = dict(table=self.name, _debug=query)
                raise GeneFabConfigurationException(msg, **_kw)
            else:
                msg = f"retrieved from SQLite as pandas DataFrame"
                GeneFabLogger().info(f"{self.name}; {msg}")
                data.columns = self.columns
                return DataDataFrame(data)
