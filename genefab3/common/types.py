from collections.abc import Callable
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import wraps
from pandas import DataFrame, MultiIndex, Index
from numpy import dtype


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
 
    def __init__(self, genefab3_client, *, mongo_collections, locale, sqlite_dbs, adapter):
        self.genefab3_client = genefab3_client
        self.mongo_collections, self.locale = mongo_collections, locale
        self.sqlite_dbs, self.adapter = sqlite_dbs, adapter
 
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


class AnnotationDataFrame(GeneFabDataFrame):
    @property
    def accessions(self):
        col = ("id", "accession")
        return set(self[col].drop_duplicates()) if col in self else set()
    @property
    def metadata_columns(self):
        return [c for c in self.columns if c[0] not in {"id", "file"}]
    @property
    def cls_valid(self):
        return len(self.metadata_columns) == 1


class DataDataFrame(GeneFabDataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata.extend(["datatypes", "gct_validity_set"])
        self.datatypes, self.gct_validity_set = set(), set()
    @property
    def accessions(self):
        return set(self.columns[1:].get_level_values(0))
    @property
    def gct_valid(self):
        return (
            (len(self.datatypes) == 1) and
            self.gct_validity_set and all(self.gct_validity_set)
        )
