from math import nan
from collections.abc import Callable
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import wraps
from genefab3.common.utils import RewindableIterator, blackjack, KeyToPosition


class SuperchargedNaN(float):
    """math.nan represented as NaN and comparable against any types"""
    def __new__(self): return float.__new__(self, nan)
    def __str__(self): return "NaN"
    def __repr__(self): return "NaN"
    def __eq__(self, other): return False
    def __lt__(self, other): return not isinstance(other, float)
    def __gt__(self, other): return False
NaN = SuperchargedNaN()


class Adapter():
    """Base class for database adapters"""
 
    def __init__(self):
        """Validate subclassed Adapter"""
        for method_name in "get_accessions", "get_files_by_accession":
            if not isinstance(getattr(self, method_name, None), Callable):
                msg = "Adapter must define method"
                _kw = dict(adapter=type(self).__name__, method=method_name)
                raise GeneFabConfigurationException(msg, **_kw)
 
    def get_favicon_urls(self):
        """List favicon URLs, of which the first reachable one will be used (fallback behavior)"""
        return []
 
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


class StreamedTable():
    """Generalized streamed table (either from MongoDB or from SQLite)"""
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
 
    def _schemify(self, target, isinstance=isinstance, type=type, float=float, str=str, SuperchargedNaN=SuperchargedNaN, len=len, TypeError=TypeError, enumerate=enumerate, zip=zip, min=min, max=max):
        """Aggregate and return (yield once) value descriptors (type, min, max, hasnan) for each column"""
        _mt = lambda a, b, bool=bool, type=type, isinstance=isinstance: (bool if
            isinstance(a, bool) and isinstance(b, bool) else type(a + b))
        for i, level in enumerate(target):
            if i == 0:
                minima, maxima = level[:], level[:]
                types, hasnan = [str] * len(level), [a != a for a in level]
            else:
                for j, (_min, _max, b) in enumerate(zip(minima, maxima, level)):
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
            if _t is SuperchargedNaN:
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
 
    def __init__(self, *, cursor, category_order=("investigation", "study", "assay", "file"), na_rep=NaN, normalization_level=2):
        """Make and retain forked aggregation cursors, infer index names and columns adhering to provided category order"""
        self._cursor, self._na_rep = RewindableIterator(cursor), na_rep
        self.accessions, _key_pool, _nrows = set(), set(), 0
        for _nrows, entry in enumerate(self._cursor.rewound(), 1):
            for key, value in blackjack(entry, max_level=normalization_level):
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
        yield from self._iter_body_levels(self._cursor.rewound(), dispatcher)
 
    @property
    def values(self):
        """Iterate values line by line, like in pandas"""
        dispatcher = self._column_key_dispatcher
        yield from self._iter_body_levels(self._cursor.rewound(), dispatcher)


class StreamedDataTable(StreamedTable):
    pass


# Legacy classes, to be removed after full refactoring:


from pandas import DataFrame, MultiIndex, Index
from numpy import dtype
from genefab3.common.logger import GeneFabLogger


class GeneFabDataFrame(DataFrame):
    @property
    def schema(self):
        """Represent each column as {type}[({min})..({max})|{has_nans}]"""
        def _column_schema(column):
            if column.dtype in (dtype("int64"), dtype("float64")):
                _t, _min, _max = str(column.dtype)[:-2], None, None
                if _t == "float":
                    _dropna = column.dropna()
                    _int = _dropna.astype(int)
                    if (_dropna == _int).all():
                        _t, _min, _max = "int", _int.min(), _int.max()
                if (_min is None) or (_max is None):
                    _min, _max = column.min(), column.max()
                _nan = "|NaN" if column.isnull().any() else ""
                return f"{_t}[({_min})..({_max}){_nan}]"
            else:
                _t = "bool" if column.dtype is dtype("bool") else "str"
                return f"{_t}[..|NaN]" if column.isnull().any() else f"{_t}[..]"
        schema = self.apply(_column_schema).to_frame().T
        if isinstance(self.index, MultiIndex):
            index_frame = self.index.to_frame(index=False)
            schema.index = MultiIndex.from_frame(
                index_frame.apply(_column_schema).to_frame().T
            )
        else:
            schema.index = Index(
                [_column_schema(self.index)], name=self.index.name,
            )
        return type(self)(schema)


class DataDataFrame(GeneFabDataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata.extend(["datatypes", "gct_validity_set"])
        self.datatypes, self.gct_validity_set = set(), set()
    @property
    def accessions(self):
        if self.columns.nlevels == 2:
            # may have been squashed for '&format=browser':
            accessions = set()
            for c in self.columns[1:].get_level_values(0):
                if c.count("<br>") < 2:
                    accessions.add(c.split("<br>")[0])
                else:
                    msg = "DataDataFrame.accessions: unexpected column name"
                    GeneFabLogger().warning(f"{msg}: {c!r}")
                    return None
            else:
                return accessions
        else: # normal representation
            return set(self.columns[1:].get_level_values(0))
    @property
    def gct_valid(self):
        return (
            (len(self.datatypes) == 1) and
            self.gct_validity_set and all(self.gct_validity_set)
        )
