from itertools import tee
from collections.abc import Callable
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import wraps
from flask import Response
from genefab3.common.utils import blackjack, KeyToPosition
from genefab3.db.sql.utils import SQLTransactions, reraise_operational_error
from sqlite3 import OperationalError
from genefab3.common.exceptions import GeneFabLogger


class ExtNaN(float):
    """Extended nan: math.nan represented as NaN and comparable against any types"""
    def __new__(self): return float.__new__(self, "nan")
    def __str__(self): return "NaN"
    def __repr__(self): return "NaN"
    def __eq__(self, other): return False
    def __lt__(self, other): return not isinstance(other, float)
    def __gt__(self, other): return False
    def __hash__(self): return hash(float("nan"))
NaN = ExtNaN()


class PhoenixIterator():
    """Iterator factory, returns a teed copy of original iterator when asked to iterate"""
    def __init__(self, it):
        self.it = it
    def __iter__(self):
        self.it, _it = tee(self.it)
        return _it
    def __next__(self):
        for e in self:
            return e


class StringIterator():
    """Wraps functions that yield text"""
    default_format = "html"
    def __init__(self, func): self.func = func
    def __call__(self): return self.func()


class Adapter():
    """Base class for database adapters""" # TODO: documentation for `get_accessions` and `get_files_by_accession`
 
    def __init__(self):
        """Validate subclassed Adapter"""
        for method_name in "get_accessions", "get_files_by_accession":
            if not isinstance(getattr(self, method_name, None), Callable):
                msg = "Adapter must define method"
                _kw = dict(adapter=type(self).__name__, method=method_name)
                raise GeneFabConfigurationException(msg, **_kw)
 
    def best_sample_name_matches(self, name, names, return_positions=False):
        """Test sample name identity (fallback behavior)"""
        if return_positions:
            positions_and_matches = [
                (p, ns) for p, ns in enumerate(names) if ns == name
            ]
            return (
                [ns for p, ns in positions_and_matches],
                [p for p, ns in positions_and_matches],
            )
        else:
            return [ns for ns in names if ns == name]


class Routes():
    """Base class for registered endpoints"""
 
    def __init__(self, genefab3_client):
        self.genefab3_client = genefab3_client
 
    def register_endpoint(endpoint=None):
        """Decorator that adds `endpoint` and `fmt` attributes to class method"""
        def outer(method):
            @wraps(method)
            def inner(*args, **kwargs):
                return method(*args, **kwargs)
            if endpoint:
                inner.endpoint = endpoint
            elif hasattr(method, "__name__"):
                if isinstance(method.__name__, str):
                    inner.endpoint = "/" + method.__name__ + "/"
            return inner
        return outer
 
    def items(self):
        """Iterate over methods of `Routes` object that have `endpoint` attribute"""
        for name in dir(self):
            method = getattr(self, name)
            if isinstance(getattr(method, "endpoint", None), str):
                yield method.endpoint, method


class ResponseContainer():
    """Holds content (bytes, strings, streamer function, or Response), mimetype, and originating object"""
    def update(self, content=None, mimetype=None, obj=None):
        self.content, self.mimetype, self.obj = content, mimetype, obj
    def __init__(self, content=None, mimetype=None, obj=None):
        self.update(content, mimetype, obj)
    @property
    def empty(self):
        return self.content is None
    def make_response(self):
        if isinstance(self.content, Response):
            return self.content
        elif isinstance(self.content, Callable):
            return Response(self.content(), mimetype=self.mimetype)
        elif self.content is not None:
            return Response(self.content, mimetype=self.mimetype)
        else:
            msg = "Route returned no response"
            raise GeneFabConfigurationException(msg)


class StreamedTable():
    """Generalized streamed table (either from MongoDB or from SQLite)"""
    default_format = "csv"
    cacheable = True
    def placeholder(self, *, n_column_levels):
        return type("EmptyStreamedTable", (type(self),), dict(
            __init__=lambda *a, **k: None, shape=(0, 0),
            move_index_boundary=lambda *a, **k: None,
            index_levels=["*"], column_levels=["*"] * n_column_levels,
            n_index_levels=1, index=[[NaN]], values=[[NaN]],
            __getattr__=lambda s, a: (),
        ))()
    @property
    def schema(self): return StreamedSchema(self)
    @property
    def index_names(self): yield from zip(*list(self.index_levels))
    @property
    def columns(self): yield from zip(*list(self.column_levels))


class StreamedSchema(StreamedTable):
    """Streamed value descriptors (type, min, max, hasnan) per column of StreamedTable"""
 
    def __init__(self, table): self.table = table
    def __getattr__(self, attr): return getattr(self.table, attr)
 
    @property
    def shape(self): return (1, self.table.shape[1])
    @property
    def index(self): yield from self._schemify(self.table.index)
    @property
    def values(self): yield from self._schemify(self.table.values)
 
    def _schemify(self, target, isinstance=isinstance, str=str, min=min, max=max, TypeError=TypeError, zip=zip, ExtNaN=ExtNaN, float=float):
        """Aggregate and return (yield once) value descriptors (type, min, max, hasnan) for each column"""
        _mt = lambda a, b, bool=bool, type=type, isinstance=isinstance: (bool if
            isinstance(a, bool) and isinstance(b, bool) else type(a + b))
        _zip_enum = lambda *a, enumerate=enumerate, zip=zip: enumerate(zip(*a))
        if self.table.shape[0] == 0:
            yield [NaN] * self.table.shape[1]
        for i, level in enumerate(target):
            if i == 0:
                minima, maxima = list(level), list(level)
                types, hasnan = [str] * len(level), [a != a for a in level]
            else:
                for j, (_min, _max, b) in _zip_enum(minima, maxima, level):
                    if not isinstance(_min, str):
                        try:
                            minima[j], types[j] = min(_min, b), _mt(_min, b)
                        except TypeError:
                            minima[j], types[j] = "str", str
                    if not isinstance(_max, str):
                        try:
                            maxima[j] = max(_max, b)
                            types[j] = str if types[j] is str else _mt(_max, b)
                        except TypeError:
                            maxima[j], types[j] = "str", str
                hasnan = [a or (b != b) for a, b in zip(hasnan, level)]
        _schema = []
        for _t, _min, _max, _h in zip(types, minima, maxima, hasnan):
            _pipenan = f"|{NaN}" if _h else ""
            if _t is ExtNaN:
                _t = float
            if _t is str:
                _schema.append(f"str[..{_pipenan}]")
            else:
                _schema.append(f"{_t.__name__}[({_min})..({_max}){_pipenan}]")
        yield _schema


class StreamedAnnotationTable(StreamedTable):
    """Table streamed from MongoDB cursor or cursor-like iterator"""
    _index_category = "id"
    _accession_key = "id.accession"
    _isa_categories = {"investigation", "study", "assay"}
 
    def __init__(self, *, cursor, category_order=("investigation", "study", "assay", "file"), na_rep=NaN):
        """Infer index names and columns adhering to provided category order, retain forked aggregation cursors"""
        self._cursor, self._na_rep = PhoenixIterator(cursor), na_rep
        self.accessions, _key_pool, _nrows = set(), set(), 0
        for _nrows, entry in enumerate(self._cursor, 1):
            for key, value in blackjack(entry, max_level=2):
                _key_pool.add(key)
                if key == self._accession_key:
                    self.accessions.add(value)
        _full_category_order = [
            self._index_category,
            *(p for p in category_order if p != self._index_category), "",
        ]
        _key_order = {p: set() for p in _full_category_order}
        for key in _key_pool:
            for category in _full_category_order:
                if key.startswith(category):
                    _key_order[category].add(key)
                    break
        self._index_key_dispatcher = KeyToPosition(
            sorted(_key_order[self._index_category]),
        )
        self._column_key_dispatcher = KeyToPosition(
            *(sorted(_key_order[p]) for p in _full_category_order[1:]),
        )
        self.n_index_levels = len(self._index_key_dispatcher)
        self.shape = (_nrows, len(self._column_key_dispatcher))
 
    def move_index_boundary(self, *, to):
        """Like pandas methods reset_index() and set_index(), but by numeric position"""
        keys = iter([*self._index_key_dispatcher, *self._column_key_dispatcher])
        index_keys = (next(keys) for _ in range(to))
        self._index_key_dispatcher = KeyToPosition(index_keys)
        self._column_key_dispatcher = KeyToPosition(keys)
        self.shape = (self.shape[0], len(self._column_key_dispatcher))
        self.n_index_levels = len(self._index_key_dispatcher)
 
    def _iter_header_levels(self, dispatcher):
        fields_and_bounds = [
            (ff, (ff[0] in self._isa_categories) + 1)
            for ff in (c.split(".") for c in dispatcher)
        ]
        yield [".".join(ff[:b]) or "*" for ff, b in fields_and_bounds]
        yield [".".join(ff[b:]) or "*" for ff, b in fields_and_bounds]
 
    @property
    def index_levels(self):
        """Iterate index level line by line"""
        yield from self._iter_header_levels(self._index_key_dispatcher)
 
    @property
    def column_levels(self):
        """Iterate column levels line by line"""
        yield from self._iter_header_levels(self._column_key_dispatcher)
 
    @property
    def metadata_columns(self, _getcat=lambda c: c[0].split(".")[0]):
        """List columns under any ISA category"""
        return [c for c in self.columns if _getcat(c) in self._isa_categories]
 
    @property
    def cls_valid(self):
        """Test if valid for CLS, i.e. has exactly one metadata column"""
        return len(self.metadata_columns) == 1
 
    def _iter_body_levels(self, cursor, dispatcher):
        for entry in cursor:
            level = [self._na_rep] * len(dispatcher)
            for key, value in blackjack(entry, max_level=2):
                if key in dispatcher:
                    level[dispatcher[key]] = value
            yield level
 
    @property
    def index(self):
        """Iterate index line by line, like in pandas"""
        dispatcher = self._index_key_dispatcher
        yield from self._iter_body_levels(self._cursor, dispatcher)
 
    @property
    def values(self):
        """Iterate values line by line, like in pandas"""
        dispatcher = self._column_key_dispatcher
        yield from self._iter_body_levels(self._cursor, dispatcher)


class StreamedDataTable(StreamedTable):
    """Table streamed from SQLite query"""
 
    def __init__(self, *, sqlite_db, source_select, targets, query_filter, na_rep=None):
        """Infer index names and columns, retain connection and query information"""
        from genefab3.db.sql.streamed_tables import SQLiteIndexName
        _split3 = lambda c: (c[0].split("/", 2) + ["*", "*"])[:3]
        self.sqlite_db = sqlite_db
        self.source_select = source_select
        self.sqltransactions = SQLTransactions(sqlite_db, source_select.name)
        self.targets = targets
        self.query_filter = query_filter
        self.na_rep = na_rep
        self.query = f"""
            SELECT {targets} FROM `{source_select.name}` {query_filter}
        """
        desc = "tables/StreamedDataTable"
        with self.sqltransactions.concurrent(desc) as (connection, execute):
            try:
                cursor = connection.cursor()
                cursor.execute(self.query)
                self._index_name = SQLiteIndexName(cursor.description[0][0])
                self._columns = [_split3(c) for c in cursor.description[1:]]
                _count_query = f"SELECT count(*) FROM ({self.query})"
                _nrows = (execute(_count_query).fetchone() or [0])[0]
                self.shape = (_nrows, len(self._columns))
            except OperationalError as e:
                reraise_operational_error(self, e)
        self.accessions = {c[0] for c in self._columns}
        self.n_index_levels = 1
        self.datatypes, self.gct_validity_set = set(), set()
 
    @property
    def gct_valid(self):
        """Test if valid for GCT, i.e. has exactly one datatype, and the datatype is supported"""
        return (
            (len(self.datatypes) == 1) and
            self.gct_validity_set and all(self.gct_validity_set)
        )
 
    def move_index_boundary(self, *, to):
        """Like pandas methods reset_index() and set_index(), but by numeric position"""
        if to == 0:
            self.n_index_levels = 0
            self.shape = (self.shape[0], len(self._columns) + 1)
        elif to == 1:
            self.n_index_levels = 1
            self.shape = (self.shape[0], len(self._columns))
        else:
            msg = "StreamedDataTable.move_index_boundary() only moves to 0 or 1"
            raise GeneFabConfigurationException(msg, to=to)
 
    @property
    def index_levels(self):
        """Iterate index level line by line"""
        if self.n_index_levels:
            yield ["*", "*", self._index_name]
 
    @property
    def column_levels(self):
        """Iterate column levels line by line"""
        if self.n_index_levels:
            yield from zip(*self._columns)
        else:
            yield from zip(["*", "*", self._index_name], *self._columns)
 
    @property
    def index(self):
        """Iterate index line by line, like in pandas"""
        if self.n_index_levels:
            index_query = f"SELECT `{self._index_name}` FROM ({self.query})"
            desc = "tables/StreamedDataTable/index"
            with self.sqltransactions.concurrent(desc) as (_, execute):
                try:
                    if self.na_rep is None:
                        yield from execute(index_query)
                    else:
                        _na_tup = (self.na_rep,)
                        for value, *_ in execute(index_query):
                            yield _na_tup if value is None else (value,)
                except OperationalError as e:
                    reraise_operational_error(self, e)
        else:
            yield from ([] for _ in range(self.shape[0]))
 
    @property
    def values(self):
        """Iterate values line by line, like in pandas"""
        desc = "tables/StreamedDataTable/values"
        try:
            if self.na_rep is None:
                if self.n_index_levels:
                    with self.sqltransactions.concurrent(desc) as (_, execute):
                        for _, *vv in execute(self.query):
                            yield vv
                else:
                    with self.sqltransactions.concurrent(desc) as (_, execute):
                        yield from execute(self.query)
            else:
                if self.shape[0] > 50:
                    msg = "StreamedDataTable with custom na_rep may be slow"
                    GeneFabLogger.warning(msg)
                if self.n_index_levels:
                    with self.sqltransactions.concurrent(desc) as (_, execute):
                        for _, *vv in execute(self.query):
                            yield [self.na_rep if v is None else v for v in vv]
                else:
                    with self.sqltransactions.concurrent(desc) as (_, execute):
                        for vv in execute(self.query):
                            yield [self.na_rep if v is None else v for v in vv]
        except OperationalError as e:
            reraise_operational_error(self, e)
