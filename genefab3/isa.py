from argparse import Namespace
from genefab3.exceptions import GeneLabJSONException
from functools import partial


Any, Atom = "Any", "Atom"


def descend(_using, _via, _lengths):
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


def isatomic(variable):
    for x in variable:
        if isinstance(x, (dict, list)):
            return False
    else:
        return True


def populate(_what=Namespace(), _using={}, _via=None, _lengths=Any, _toplevel_method=None, _each=None, _copy_atoms=True, _copy_atomic_lists=True, _raised=(), **kwargs):
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
            elif isinstance(v, list) and _copy_atomic_lists and isatomic(v):
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
    return partial(
        populate, _via=_via, _lengths=_lengths, _each=_each,
        _toplevel_method=_toplevel_method, **kwargs,
    )


def sparse_json_to_dataframe(entries):
    return repr(entries)[:50] + "..."


def sparse_json_to_many_dataframes(_using, ignore=(AttributeError,)):
    try:
        return {k: sparse_json_to_dataframe(e) for k, e in _using.items()}
    except ignore:
        pass


class ISA(Namespace):
    def __init__(self, json):
        parser = Parser([None, 0], [1, Any],
            doi=Parser(["doiFields", 0, "doi"], [1, Any, Atom]),
            _raised=Parser(["foreignFields", 0, "isa2json"], [1, Any, Any],
                _copy_atoms=False,
                _raised=Parser("additionalInformation", Any,
                    _raised=Parser(_each=sparse_json_to_many_dataframes),
                    assays2=Parser("assays", 1, sparse_json_to_many_dataframes),
                ),
            ),
        )
        parser(_what=self, _using=json)
