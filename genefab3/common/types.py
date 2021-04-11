from collections import defaultdict
from collections.abc import Callable
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import wraps


NestedDefaultDict = lambda: defaultdict(NestedDefaultDict)


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
 
    def register_endpoint(*, endpoint=None, fmt="tsv", cache=True):
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
            inner.fmt, inner.cache = fmt, cache
            return inner
        return outer
 
    def items(self):
        """Iterate over methods of `Routes` object that have `endpoint` attribute"""
        for name in dir(self):
            method = getattr(self, name)
            if isinstance(getattr(method, "endpoint", None), str):
                yield method.endpoint, method
