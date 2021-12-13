from numpy import generic as NumpyGenericType, base_repr
from datetime import datetime
from copy import deepcopy
from re import search, sub, compile
from genefab3.common.exceptions import GeneFabParserException
from functools import partial, reduce
from urllib.request import quote
from base64 import b64encode
from uuid import uuid3, uuid4
from contextlib import contextmanager
from genefab3.common.exceptions import GeneFabLogger
from requests import get as request_get
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabConfigurationException
from operator import getitem
from collections import defaultdict
from threading import Thread


timestamp36 = lambda: base_repr(int(datetime.now().timestamp() * (10**6)), 36)
as_is = lambda _:_
EmptyIterator = lambda *a, **k: []

copy_except = lambda d, *kk: {k: v for k, v in d.items() if k not in kk}
deepcopy_except = lambda d, *kk: deepcopy({k: d[k] for k in d if k not in kk})
deepcopy_keys = lambda d, *kk: deepcopy({k: d[k] for k in kk})

is_regex = lambda v: search(r'^\/.*\/$', v)
repr_quote = partial(quote, safe=" /'\",;:[{}]=")
space_quote = partial(quote, safe=" /")


def make_safe_token(token, allow_regex=False):
    """Quote special characters, ensure not a $-command. Note: SQL queries are sanitized in genefab3.db.sql.streamed_tables"""
    quoted_token = space_quote(token)
    if allow_regex and ("$" not in sub(r'\$\/$', "", quoted_token)):
        return quoted_token
    elif "$" not in quoted_token:
        return quoted_token
    else:
        raise GeneFabParserException("Forbidden argument", field=quoted_token)


def random_unique_string(seed=""):
    return b64encode(uuid3(uuid4(), seed).bytes, b'_-').decode().rstrip("=")


@contextmanager
def pick_reachable_url(urls, name=None):
    """Iterate `urls` and get the first reachable URL"""
    def _pick():
        get_kws = dict(allow_redirects=True, stream=True)
        for url in urls:
            GeneFabLogger.debug(f"Trying URL: {url}")
            try:
                with request_get(url, **get_kws) as response:
                    if response.ok:
                        GeneFabLogger.debug(f"Hit URL: {url}")
                        return url
            except (URLError, OSError):
                GeneFabLogger.debug(f"Unreachable URL: {url}")
                continue
        else:
            if name:
                raise URLError(f"No URLs are reachable for {name}: {urls}")
            else:
                raise URLError(f"No URLs are reachable: {urls}")
    yield _pick()


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


def json_permissive_default(o):
    """Serialize numpy entries as native types, sets as informative strings, other unserializable entries as their type names"""
    if isinstance(o, NumpyGenericType):
        return o.item()
    elif isinstance(o, set):
        return f"<set>{list(o)}"
    elif isinstance(o, bytes):
        return str(o)
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
