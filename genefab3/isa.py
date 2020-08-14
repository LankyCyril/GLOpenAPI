from argparse import Namespace
from genefab3.exceptions import GeneLabJSONException
from collections import Callable
from functools import partial


ANY, ATOM = "any", "atom"


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
        if _len != ANY:
            error_mask = "Unexpected number of fields under key {}"
            if isinstance(source, (list, dict)):
                if (len(source) != _len):
                    raise GeneLabJSONException(error_mask.format(key))
            elif _len != ATOM:
                raise GeneLabJSONException(error_mask.format(key))
    return source


def atomic(value):
    for v in value:
        if isinstance(v, (dict, list)):
            return False
    else:
        return True


def populate(_what=Namespace(), _using={}, _via=(None,), _lengths=(ANY,), _method=None, _copy_atoms=True, _copy_atomic_lists=True, _raised=(), **kwargs):
    if (not _using) or (len(_via) == 0):
        raise GeneLabJSONException("Reached a dead end in JSON")
    source = descend(_using, _via, _lengths)
    if _method is not None:
        return _method(source)
    elif isinstance(source, list):
        error_mask = "Could not get key-value pairs at level {}"
        raise GeneLabJSONException(error_mask.format(_via))
    elif isinstance(source, dict):
        for method in _raised:
            for key, value in method(_using=source)._get_kwargs():
                setattr(_what, key, value)
        for key, value in source.items():
            if isinstance(value, list) and _copy_atomic_lists and atomic(value):
                setattr(_what, key, value)
            elif (not isinstance(value, (dict, list))) and _copy_atoms:
                setattr(_what, key, value)
        for key, method in kwargs.items():
            if isinstance(method, Callable):
                setattr(_what, key, method(_using=source))
            else:
                setattr(_what, key, method)
        return _what
    elif kwargs:
        raise GeneLabJSONException("Cannot descend into keys of atomic field")
    else:
        return source


def staged_populate(_via=(None,), _lengths=(ANY,), _method=None, **kwargs):
    return partial(
        populate, _via=_via, _lengths=_lengths, _method=_method, **kwargs,
    )


def sparse_json_to_dataframe(entries):
    return repr(entries)[:50] + "..."


def sparse_json_to_many_dataframes(dict_of_entries):
    return {
        key: sparse_json_to_dataframe(entries)
        for key, entries in dict_of_entries.items()
    }


class ISA(Namespace):
 
    def __init__(self, json):
        populate(
            _what=self, _using=json, _via=(None, 0), _lengths=(1, ANY),
            _raised=[
                staged_populate(
                    ("foreignFields", 0, "isa2json"), (1, ANY, ANY),
                ),
                staged_populate(
                    ("foreignFields", 0, "isa2json", "additionalInformation"),
                    (1, ANY, ANY, ANY),
                    assays=staged_populate(
                        ("assays",), (ANY,), sparse_json_to_many_dataframes,
                    ),
                    samples=staged_populate(
                        ("samples",), (ANY,), sparse_json_to_many_dataframes,
                    ),
                ),
            ],
            doi=staged_populate(
                ("doiFields", 0, "doi"), (1, ANY, ATOM),
            ),
        )
