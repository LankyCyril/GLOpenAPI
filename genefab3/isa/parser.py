from argparse import Namespace
from genefab3.exceptions import GeneLabJSONException
from functools import partial
from genefab3.isa.types import FromSparseTable


Any, Atom = "Any", "Atom"


def descend(_using, _via, _lengths):
    """Follow given keys and indices down into JSON, check value lengths during descent"""
    if len(_via) != len(_lengths):
        raise GeneLabJSONException("Unexpected number of requested JSON fields")
    source = _using
    for key, _len in zip(_via, _lengths):
        if key is not None:
            try:
                source = source[key]
            except (IndexError, KeyError):
                error_mask = "Could not descend into key {}"
                raise GeneLabJSONException(error_mask.format(key))
        if _len != Any:
            error_mask = "Unexpected number of fields under key {}"
            if isinstance(source, (list, dict)):
                if (len(source) != _len):
                    raise GeneLabJSONException(error_mask.format(key))
            elif _len != Atom:
                raise GeneLabJSONException(error_mask.format(key))
    return source


def isatomiclist(variable):
    """Check if `variable` is list of atomic items"""
    for x in variable:
        if isinstance(x, (dict, list)):
            return False
    else:
        return True


class DefaultNamespace(Namespace):
    """Namespace with infinite descent"""
    def __getattr__(self, x):
        return getattr(super(), x, DefaultNamespace())


def populate(_what=DefaultNamespace(), _using={}, _via=None, _lengths=Any, _toplevel_method=None, _each=None, _copy_atoms=False, _copy_atomic_lists=False, _raised=(), **kwargs):
    """Parse entries at given level of JSON"""
    if not isinstance(_via, (list, tuple)):
        _via = [_via]
    if not isinstance(_lengths, (list, tuple)):
        _lengths = [_lengths]
    if not isinstance(_raised, (list, tuple)):
        _raised = [_raised]
    if (not _using) or (len(_via) == 0):
        raise GeneLabJSONException("Reached a dead end in JSON")
    source = descend(_using, _via, _lengths)
    if _toplevel_method is not None:
        return _toplevel_method(source)
    elif isinstance(source, list):
        error_mask = "Could not get key-value pairs at level {}"
        raise GeneLabJSONException(error_mask.format(_via))
    elif isinstance(source, dict):
        for method in _raised:
            for k, v in method(_using=source)._get_kwargs():
                setattr(_what, k, v)
        for k, v in source.items():
            if _each is not None:
                setattr(_what, k, _each(v))
            elif isinstance(v, list) and _copy_atomic_lists and isatomiclist(v):
                setattr(_what, k, v)
            elif (not isinstance(v, (dict, list))) and _copy_atoms:
                setattr(_what, k, v)
        for k, method in kwargs.items():
            if callable(method):
                setattr(_what, k, method(_using=source))
            else:
                setattr(_what, k, method)
        return _what
    elif kwargs:
        raise GeneLabJSONException("Cannot descend into keys of atomic field")
    else:
        return source


def Parser(_via=None, _lengths=Any, _toplevel_method=None, _each=None, **kwargs):
    """Stage populate() with some arguments filled"""
    return partial(
        populate, _via=_via, _lengths=_lengths, _each=_each,
        _toplevel_method=_toplevel_method, **kwargs,
    )


def valmapper(function, ignore=AttributeError):
    """Like toolz.valmap, but decorator-like and safe if passed object is not dict"""
    def mapper(_using):
        try:
            return {k: function(e) for k, e in _using.items()}
        except ignore:
            return None
    return mapper


class ISA(DefaultNamespace):
    """Parses GLDS JSON in ISA-Tab-like fashion"""
    def __init__(self, json):
        parser = Parser([None, 0], [1, Any],
            _copy_atoms=True, _copy_atomic_lists=True,
            doi=Parser(["doiFields", 0, "doi"], [1, Any, Atom]),
            _raised=Parser(["foreignFields", 0, "isa2json"], [1, Any, Any],
                _raised=Parser("additionalInformation", Any,
                    assays=Parser("assays", 1, valmapper(FromSparseTable)),
                    samples=Parser("samples", 1, valmapper(FromSparseTable)),
                ),
            ),
        )
        parser(_what=self, _using=json)
