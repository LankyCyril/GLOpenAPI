from collections import defaultdict
from werkzeug.datastructures import ImmutableDict
from numpy import nan
from collections.abc import Hashable, Callable
from itertools import zip_longest
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import wraps


class UniversalSet(set):
    """Naive universal set"""
    def __and__(self, x): return x
    def __iand__(self, x): return x
    def __rand__(self, x): return x
    def __or__(self, x): return self
    def __ior__(self, x): return self
    def __ror__(self, x): return self
    def __contains__(self, x): return True


NestedDefaultDict = lambda: defaultdict(NestedDefaultDict)


def ImmutableTree(d, step_tracker=1, max_steps=256):
    """Converts nested dictionaries, lists, tuples into immutable equivalents"""
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
    """Provides facilities to describe equality within a class based on a subset of fields"""
 
    def __init__(self, identity_fields, as_strings=()):
        """Describe equality within a class based on a subset of fields"""
        self.__identity_fields = tuple(identity_fields)
        self.__as_strings = set(as_strings)
 
    def __iter_identity_values__(self):
        """Iterate values of identity fields in a hash-friendly manner"""
        for field in self.__identity_fields:
            value = getattr(self, field, nan)
            if field in self.__as_strings:
                value = str(value)
            if not isinstance(value, Hashable):
                msg = "{}: unhashable field value".format(type(self).__name__)
                raise TypeError(msg, f"{field}={repr(value)}")
            else:
                yield value
 
    def __eq__(self, other):
        """Compare values of identity fields between self and other"""
        return all(s == o for s, o in zip_longest(
            self.__iter_identity_values__(),
            getattr(other, "__iter_identity_values__", lambda: ())(),
            fillvalue=nan,
        ))
 
    def __hash__(self):
        """Hash values of identity fields as a tuple"""
        return hash(tuple(self.__iter_identity_values__()))


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
