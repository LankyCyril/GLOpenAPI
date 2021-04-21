from os import environ
from contextlib import contextmanager
from requests import head as request_head
from urllib.request import urlopen
from urllib.error import URLError
from re import compile, escape
from copy import deepcopy
from pandas import DataFrame
from genefab3.common.exceptions import GeneFabConfigurationException
from functools import partial, reduce
from operator import getitem
from collections import defaultdict, OrderedDict
from marshal import dumps as marsh


as_is = lambda _:_
empty_iterator = lambda *a, **k: []


def is_debug():
    """Determine if app is running in debug mode"""
    return (
        environ.get("FLASK_ENV", None)
        in {"development", "staging", "stage", "debug", "debugging"}
    )


def is_flask_reloaded():
    """https://stackoverflow.com/a/9476701/590676"""
    return (environ.get("WERKZEUG_RUN_MAIN", None) == "true")


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


def blackjack_items(e, max_depth, head, marsh=marsh, len=len, isinstance=isinstance, dict=dict, sum=sum, tuple=tuple, join=".".join, cache=OrderedDict()):
    """Quickly iterate flattened dictionary key-value pairs of known schema in pure Python, with LRU caching"""
    ck = marsh(e, 4), max_depth, head
    if ck not in cache:
        if len(cache) >= 65536:
            cache.popitem(0)
        if isinstance(e, dict):
            if len(head) < max_depth:
                cache[ck] = sum((tuple(blackjack_items(v, max_depth, head+(k,)))
                    for k, v in e.items()), ())
            else:
                cache[ck] = ((join(head), e.get("", e)),)
        else:
            cache[ck] = ((join(head), e),)
    yield from cache[ck]


def blackjack_normalize(cursor, max_depth=3, dict=dict, blackjack_items=blackjack_items):
    """Quickly flatten iterable of dictionaries of known schema in pure Python"""
    return DataFrame(dict(blackjack_items(e, max_depth, ())) for e in cursor)
