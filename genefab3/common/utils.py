from contextlib import contextmanager
from requests import head as request_head
from urllib.request import urlopen
from urllib.error import URLError
from uuid import uuid3, uuid4
from os import path
from re import sub, escape, split
from copy import deepcopy
from pandas import DataFrame, Series
from genefab3.common.exceptions import GeneFabConfigurationException
from json import JSONEncoder


leaf_count = lambda d: sum(len(v) for v in d.values())
as_is = lambda _:_
empty_iterator = lambda *a, **k: []


@contextmanager
def pick_reachable_url(urls, desc=None):
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
                if desc:
                    raise URLError(f"No URLs are reachable for {desc}: {urls}")
                else:
                    raise URLError(f"No URLs are reachable: {urls}")
    yield _pick()


def urn(s):
    """Generate uuid3 URN for `s`"""
    return uuid3(uuid4(), s).urn


def walk_up(from_path, n_steps):
    """Get path of directory `n_steps` above `from_path`"""
    if n_steps >= 1:
        return walk_up(path.split(from_path)[0], n_steps-1)
    else:
        return from_path


def map_replace(string, mappings):
    """Perform multiple replacements in one go"""
    return sub(
        r'|'.join(map(escape, mappings.keys())),
        lambda m: mappings[m.group()],
        string,
    )


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


def set_attributes(dataframe, **kwargs):
    """Add custom attributes to dataframe"""
    if not isinstance(dataframe, DataFrame):
        raise GeneFabConfigurationException("set_attributes(): not a DataFrame")
    else:
        for a, v in kwargs.items():
            try:
                dataframe._metadata.append(a)
                setattr(dataframe, a, v)
            except AttributeError:
                msg = f"Cannot set attribute {a} of DataFrame"
                raise GeneFabConfigurationException(msg)


def get_attribute(dataframe, a):
    """Retrieve custom attribute of dataframe"""
    if not isinstance(dataframe, DataFrame):
        raise GeneFabConfigurationException("get_attribute(): not a DataFrame")
    else:
        value = getattr(dataframe, a, None)
        if isinstance(value, (Series, DataFrame)):
            return None
        else:
            return value


def iterate_terminal_leaves(d, step_tracker=1, max_steps=256):
    """Descend into branches breadth-first and iterate terminal leaves"""
    if step_tracker >= max_steps:
        raise GeneFabConfigurationException(
            "Document branch exceeds nestedness threshold",
            max_steps=max_steps,
        )
    else:
        if isinstance(d, dict):
            for i, branch in enumerate(d.values(), start=1):
                yield from iterate_terminal_leaves(branch, step_tracker+i)
        else:
            yield d


def iterate_terminal_leaf_elements(d, sep=r'\s*,\s'):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    for value in iterate_terminal_leaves(d):
        if isinstance(value, str):
            yield from split(sep, value)


class JSONByteEncoder(JSONEncoder):
    """Allow dumps to convert sets to serializable lists"""
    def default(self, entry):
        if isinstance(entry, bytes):
            return entry.decode(errors="replace")
        else:
            return JSONEncoder.default(self, entry)
