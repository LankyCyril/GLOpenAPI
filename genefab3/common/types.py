from functools import wraps
from collections.abc import Callable
from genefab3.common.exceptions import GeneFabConfigurationException
from pandas import DataFrame


class Adapter():
    """Base class for database adapters"""
 
    def __init__(self):
        """Validate subclassed Adapter"""
        for method_name in "get_accessions", "get_files_by_accession":
            if not isinstance(getattr(self, method_name, None), Callable):
                msg = "Adapter must define method"
                _kw = dict(adapter=type(self).__name__, method=method_name)
                raise GeneFabConfigurationException(msg, **_kw)
 
    def best_sample_name_matches(self, name, names, return_positions=False):
        """Fallback sample name identity test"""
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
 
    def __init__(self, mongo_collections, *, locale, sqlite_dbs, adapter):
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


class AnnotationDataFrame(DataFrame):
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


class DataDataFrame(DataFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata.append("datatypes")
        self.datatypes = set()
    @property
    def accessions(self):
        return set(self.columns[1:].get_level_values(0))
    @property
    def gct_valid(self):
        return (len(self.datatypes) == 1) and (next(iter(self.datatypes)) in {
            # TODO: move to adapter, accommodate logic
            "processed microarray data", "normalized counts",
            "unnormalized counts",
        })
