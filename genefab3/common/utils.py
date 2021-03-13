from requests import head as request_head
from urllib.request import urlopen
from urllib.error import URLError
from os import path
from re import sub, escape
from copy import deepcopy
from pandas import DataFrame, Series


def WithEitherURL(method, _urls, _target_arg="url", **kwargs):
    """Iterate `urls` and call `method` with the first reachable URL"""
    for url in _urls:
        with request_head(url, allow_redirects=True) as response:
            if response.ok:
                return method(**kwargs, **{_target_arg: url})
    else:
        for url in _urls:
            try:
                urlopen(url)
            except URLError:
                continue
            else:
                return method(**kwargs, **{_target_arg: url})
        else:
            raise URLError(f"No URLs are reachable: {_urls}")


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


def copy_and_drop(d, keys):
    """Deepcopy dictionary `d`, delete `d[key] for key in keys`"""
    d_copy = deepcopy(d)
    for key in keys:
        del d_copy[key]
    return d_copy


def INPLACE_set_attributes(dataframe, **kwargs):
    """Add custom attributes to dataframe"""
    if not isinstance(dataframe, DataFrame):
        raise TypeError("Not a DataFrame")
    else:
        for a, v in kwargs.items():
            try:
                dataframe._metadata.append(a)
                setattr(dataframe, a, v)
            except AttributeError:
                raise AttributeError(f"Cannot set attribute {a} of DataFrame")


def get_attribute(dataframe, a):
    """Retrieve custom attribute of dataframe"""
    if not isinstance(dataframe, DataFrame):
        raise TypeError("Not a DataFrame")
    else:
        value = getattr(dataframe, a, None)
        if isinstance(value, (Series, DataFrame)):
            return None
        else:
            return value


def iterate_terminal_leaves(d, step_tracker=1, max_steps=256):
    """Descend into branches breadth-first and iterate terminal leaves"""
    if step_tracker >= max_steps:
        raise ValueError(
            "Dictionary exceeded nestedness threshold", max_steps,
        )
    else:
        if isinstance(d, dict):
            for i, branch in enumerate(d.values(), start=1):
                yield from iterate_terminal_leaves(branch, step_tracker+i)
        else:
            yield d
