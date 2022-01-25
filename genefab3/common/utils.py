from numpy import generic as NumpyGenericType, base_repr
from datetime import datetime
from copy import deepcopy
from re import compile
from functools import partial
from urllib.request import quote
from base64 import b64encode
from uuid import uuid3, uuid4
from contextlib import contextmanager
from genefab3.common.exceptions import GeneFabLogger
from requests import get as request_get
from urllib.error import URLError
from genefab3.common.exceptions import GeneFabConfigurationException
from threading import Thread


timestamp36 = lambda: base_repr(int(datetime.now().timestamp() * (10**6)), 36)
as_is = lambda _:_

copy_except = lambda d, *kk: {k: v for k, v in d.items() if k not in kk}
items_except = lambda d, *kk: ((k, v) for k, v in d.items() if k not in kk)
deepcopy_except = lambda d, *kk: deepcopy({k: d[k] for k in d if k not in kk})
deepcopy_keys = lambda d, *kk: deepcopy({k: d[k] for k in kk})

repr_quote = partial(quote, safe=" /'\",;:[{}]=")
space_quote = partial(quote, safe=" /")


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
        for i, branch in enumerate(d.values(), start=1): # TODO need enumerate, or same level for immediate children?!
            yield from iterate_terminal_leaves(branch, step_tracker+i)
    else:
        yield d


def iterate_terminal_leaf_elements(d, iter_leaves=iterate_terminal_leaves, isinstance=isinstance, str=str, pattern=compile(r'\s*,\s')):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    for value in iter_leaves(d):
        if isinstance(value, str):
            yield from pattern.split(value)


def iterate_branches_and_leaves(d, keyseq=(), value_key="", only_atomic=True, step_tracker=1, max_steps=256, isinstance=isinstance, dict=dict, str=str):
    if step_tracker >= max_steps:
        msg = "Document branch exceeds nestedness threshold"
        raise GeneFabConfigurationException(msg, max_steps=max_steps)
    elif isinstance(d, dict):
        if value_key in d:
            yield keyseq, str(d[""])
        for key, value in items_except(d, ""):
            yield from iterate_branches_and_leaves(
                value, (*keyseq, str(key)),
                value_key, only_atomic, step_tracker+1,
            )
    elif isinstance(d, str):
        yield keyseq, d
    elif not only_atomic:
        yield keyseq, str(d)


def flatten_all_keys(d, *, sep=".", head=()):
    """Recursively iterate nested dictionary `d`, yield flattened key sequences joined by `sep`"""
    for k, _d in d.items():
        if isinstance(_d, dict):
            yield from flatten_all_keys(_d, head=(*head, k))
        else:
            yield sep.join((*head, k))


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
