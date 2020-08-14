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


def atomic(variable):
    for x in variable:
        if isinstance(x, (dict, list)):
            return False
    else:
        return True


def populate(_what=Namespace(), _using={}, _via=(None,), _lengths=(ANY,), _toplevel_method=None, _per_item_method=None, _copy_atoms=True, _copy_atomic_lists=True, _raised=(), **kwargs):
    if not isinstance(_via, (list, tuple)):
        _via = [_via]
    if not isinstance(_lengths, (list, tuple)):
        _lengths = [_lengths]
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
            if _per_item_method is not None:
                setattr(_what, k, _per_item_method(v))
            elif isinstance(v, list) and _copy_atomic_lists and atomic(v):
                setattr(_what, k, v)
            elif (not isinstance(v, (dict, list))) and _copy_atoms:
                setattr(_what, k, v)
        for k, method in kwargs.items():
            if isinstance(method, Callable):
                setattr(_what, k, method(_using=source))
            else:
                setattr(_what, k, method)
        return _what
    elif kwargs:
        raise GeneLabJSONException("Cannot descend into keys of atomic field")
    else:
        return source


def StagedParser(_via=(None,), _lengths=(ANY,), _toplevel_method=None, _per_item_method=None, **kwargs):
    return partial(
        populate, _via=_via, _lengths=_lengths,
        _toplevel_method=_toplevel_method, _per_item_method=_per_item_method,
        **kwargs,
    )


def sparse_json_to_dataframe(entries):
    return repr(entries)[:50] + "..."


def sparse_json_to_many_dataframes(_using, ignore={AttributeError}):
    try:
        return {
            key: sparse_json_to_dataframe(entries)
            for key, entries in _using.items()
        }
    except Exception as e:
        if type(e) in ignore:
            pass


class ISA(Namespace):
    def __init__(self, json):
        parser = StagedParser([None, 0], [1, ANY],
            doi=StagedParser(
                ["doiFields", 0, "doi"], [1, ANY, ATOM],
            ),
            _raised=[
                StagedParser(["foreignFields", 0, "isa2json"], [1, ANY, ANY],
                    _copy_atoms=False,
                    _raised=[
                        StagedParser("additionalInformation", ANY,
                            _raised=[StagedParser(
                                _per_item_method=sparse_json_to_many_dataframes,
                            )],
                            assays_directly=StagedParser("assays", 1,
                                sparse_json_to_many_dataframes,
                            ),
                        ),
                    ],
                ),
            ],
        )
        parser(_what=self, _using=json)
