from numpy import generic as NumpyGenericType, base_repr
from datetime import datetime
from copy import deepcopy
from os import environ
from base64 import b64encode
from uuid import uuid3, uuid4
from contextlib import contextmanager
from requests import head as request_head
from urllib.request import urlopen
from urllib.error import URLError
from re import compile, escape
from pandas import DataFrame
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import partial, reduce
from operator import getitem
from collections import defaultdict, OrderedDict
from marshal import dumps as marsh
from threading import Thread


timestamp36 = lambda: base_repr(int(datetime.now().timestamp() * (10**6)), 36)
as_is = lambda _:_
EmptyIterator = lambda *a, **k: []

copy_except = lambda d, *kk: {k: v for k, v in d.items() if k not in kk}
deepcopy_except = lambda d, *kk: deepcopy({k: d[k] for k in d if k not in kk})
deepcopy_keys = lambda d, *kk: deepcopy({k: d[k] for k in kk})


def is_debug(markers={"development", "staging", "stage", "debug", "debugging"}):
    """Determine if app is running in debug mode"""
    return environ.get("FLASK_ENV", None) in markers


def random_unique_string(seed=""):
    return b64encode(uuid3(uuid4(), seed).bytes, b'_-').decode().rstrip("=")


@contextmanager
def pick_reachable_url(urls, name=None):
    """Iterate `urls` and get the first reachable URL"""
    def _pick():
        for url in urls:
            try:
                with request_head(url, allow_redirects=True) as response:
                    if response.ok:
                        return url
            except (URLError, OSError):
                continue
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


def iterate_terminal_leaves(d, step_tracker=1, max_steps=256, isinstance=isinstance, dict=dict, enumerate=enumerate):
    """Descend into branches breadth-first and iterate terminal leaves; supports arbitrary values, does not support caching"""
    if step_tracker >= max_steps:
        msg = "Document branch exceeds nestedness threshold"
        raise GeneFabConfigurationException(msg, max_steps=max_steps)
    elif isinstance(d, dict):
        for i, branch in enumerate(d.values(), start=1):
            yield from iterate_terminal_leaves(branch, step_tracker+i)
    else:
        yield d


def iterate_terminal_leaf_elements(d, iter_leaves=iterate_terminal_leaves, isinstance=isinstance, str=str, pattern=compile(r'\s*,\s')):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    for value in iter_leaves(d):
        if isinstance(value, str):
            yield from pattern.split(value)


BranchTracer = lambda sep: BranchTracerLevel(partial(BranchTracer, sep), sep)
BranchTracer.__doc__ = """Infinitely nestable and descendable defaultdict"""
class BranchTracerLevel(defaultdict):
    """Level of BranchTracer; creates nested levels by walking paths with sep"""
    def __init__(self, factory, sep):
        super().__init__(factory)
        self.split = compile(sep).split
    def descend(self, path, reduce=reduce, getitem=getitem):
        """Move one level down for each key in `path`; return terminal level"""
        return reduce(getitem, self.split(path), self)
    def make_terminal(self, truthy=True):
        """Prune descendants of current level, optionally marking self truthy"""
        self.clear()
        if truthy:
            self[True] = True # create a non-descendable element


def blackjack(e, max_level, head=(), marsh=marsh, len=len, isinstance=isinstance, dict=dict, sum=sum, tuple=tuple, join=".".join, cache=OrderedDict()):
    """Quickly iterate flattened dictionary key-value pairs of known schema in pure Python, with LRU caching"""
    ck = marsh(e, 4), max_level, head
    if ck not in cache:
        if len(cache) >= 65536:
            cache.popitem(0)
        if isinstance(e, dict):
            if len(head) <= max_level:
                cache[ck] = sum((tuple(blackjack(v, max_level, head+(k,)))
                    for k, v in e.items()), ())
            else:
                cache[ck] = ((join(head), e.get("", e)),)
        else:
            cache[ck] = ((join(head), e),)
    yield from cache[ck]


def blackjack_normalize(cursor, max_level=2, dict=dict, blackjack=blackjack):
    """Quickly flatten iterable of dictionaries of known schema in pure Python"""
    return DataFrame(dict(blackjack(e, max_level)) for e in cursor)


def json_permissive_default(o):
    """Serialize numpy entries as native types, sets as informative strings, other unserializable entries as their type names"""
    if isinstance(o, NumpyGenericType):
        return o.item()
    elif isinstance(o, set):
        return f"<set>{list(o)}"
    else:
        return str(type(o))


def validate_no_special_character(identifier, desc, c):
    """Pass through `identifier` if contains no `c`, raise GeneFabConfigurationException otherwise"""
    if (not isinstance(identifier, str)) or (c not in identifier):
        return identifier
    else:
        msg = f"{repr(c)} in {desc} name"
        raise GeneFabConfigurationException(msg, **{desc: identifier})
validate_no_backtick = partial(validate_no_special_character, c="`")
validate_no_doublequote = partial(validate_no_special_character, c='"')


class ExceptionPropagatingThread(Thread):
    """Thread that raises errors in main thread on join()"""
    def run(self):
        self.exception = None
        try:
            super(ExceptionPropagatingThread, self).run()
        except Exception as e:
            self.exception = e
            raise
    def join(self):
        super(ExceptionPropagatingThread, self).join()
        if self.exception:
            raise self.exception
