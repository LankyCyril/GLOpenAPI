from contextlib import contextmanager
from requests import head as request_head
from urllib.request import urlopen
from urllib.error import URLError
from re import compile, escape
from copy import deepcopy
from pandas import DataFrame
from pandas.core.base import PandasObject
from genefab3.common.exceptions import GeneFabConfigurationException


leaf_count = lambda d: sum(len(v) for v in d.values())
as_is = lambda _:_
empty_iterator = lambda *a, **k: []


@contextmanager
def pick_reachable_url(urls, name=None):
    """Iterate `urls` and get the first reachable URL"""
    def _pick():
        for url in urls:
            with request_head(url, allow_redirects=True) as response:
                if response.ok:
                    return url
        else:
            for url in urls:
                try:
                    urlopen(url)
                except URLError:
                    continue
                else:
                    return url
            else:
                if name:
                    raise URLError(f"No URLs are reachable for {name}: {urls}")
                else:
                    raise URLError(f"No URLs are reachable: {urls}")
    yield _pick()


def map_replace(string, mappings, compile=compile, join=r'|'.join, escape=escape, map=map):
    """Perform multiple replacements in one go"""
    pattern = compile(join(map(escape, mappings.keys())))
    return pattern.sub(lambda m: mappings[m.group()], string)


def copy_and_drop(d, drop):
    """Shallowcopy dictionary `d`, delete `d[key] for key in drop`"""
    return {k: v for k, v in d.items() if k not in drop}


def deepcopy_and_drop(d, drop):
    """Deepcopy dictionary `d`, delete `d[key] for key in drop`"""
    d_copy = deepcopy(d)
    for key in drop:
        if key in d_copy:
            del d_copy[key]
    return d_copy


def match_mapping(mapping, matchers):
    """Descend into dictionary `mapping` if keys agree with objects as defined in `matchers`"""
    dispatcher = mapping
    for method, obj in matchers:
        children = [c for k, c in dispatcher.items() if method(obj, k)]
        if len(children) == 0:
            raise KeyError
        elif len(children) > 1:
            raise ValueError
        else:
            dispatcher = children[0]
    return dispatcher


def set_attributes(obj, **kwargs):
    """Add custom attributes to object"""
    if isinstance(obj, DataFrame):
        for a in kwargs:
            obj._metadata.append(a)
    elif isinstance(obj, PandasObject):
        msg = f"Cannot set attributes to object of this type"
        raise GeneFabConfigurationException(msg, type=type(obj).__name__)
    for a, v in kwargs.items():
        try:
            setattr(obj, a, v)
        except Exception as e:
            msg = f"Cannot set this attribute to object of this type"
            _kw = dict(type=type(obj).__name__, error=e)
            raise GeneFabConfigurationException(msg, **{a: v}, **_kw)


def get_attribute(obj, a, default=None):
    """Retrieve custom attribute of object"""
    value = getattr(obj, a, default)
    if isinstance(obj, PandasObject) and isinstance(value, PandasObject):
        return default
    else:
        return value


def iterate_terminal_leaves(d, step_tracker=1, max_steps=256, isinstance=isinstance, dict=dict, enumerate=enumerate):
    """Descend into branches breadth-first and iterate terminal leaves; supports arbitrary values, does not support caching"""
    if step_tracker >= max_steps:
        msg = "Document branch exceeds nestedness threshold"
        raise GeneFabConfigurationException(msg, max_steps=max_steps)
    else:
        if isinstance(d, dict):
            for i, branch in enumerate(d.values(), start=1):
                yield from iterate_terminal_leaves(branch, step_tracker+i)
        else:
            yield d


def iterate_terminal_leaf_elements(d, iter_leaves=iterate_terminal_leaves, isinstance=isinstance, str=str, pattern=compile(r'\s*,\s')):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    for value in iter_leaves(d):
        if isinstance(value, str):
            yield from pattern.split(value)
