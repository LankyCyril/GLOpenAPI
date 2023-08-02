from os import path
from subprocess import call
from filelock import FileLock
from numpy import generic as NumpyGenericType, base_repr
from datetime import datetime
from copy import deepcopy
from re import compile, split
from typing.re import Pattern as SRE_Pattern
from json import dumps
from functools import partial
from urllib.request import quote
from base64 import b64encode
from uuid import uuid3, uuid4
from contextlib import contextmanager
from glopenapi.common.exceptions import GLOpenAPILogger
from requests import get as request_get
from urllib.error import URLError
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from threading import Thread


CYUTILS_PYX = path.join(path.dirname(__file__), "cyutils.pyx")
with FileLock(f"{CYUTILS_PYX}.lock"):
    call(["cythonize", "-i", CYUTILS_PYX])
from glopenapi.common.cyutils import blazing_json_normalize_tolist
from glopenapi.common.cyutils import blazing_json_normalize_itertuples
assert blazing_json_normalize_tolist and blazing_json_normalize_itertuples

PrimitiveTypes = (int, float, bool, str, type(None))

timestamp36 = lambda: base_repr(int(datetime.now().timestamp() * (10**6)), 36)
as_is = lambda _:_

copy_except = lambda d, *kk: {k: v for k, v in d.items() if k not in kk}
items_except = lambda d, *kk: ((k, v) for k, v in d.items() if k not in kk)
deepcopy_except = lambda d, *kk: deepcopy({k: d[k] for k in d if k not in kk})
deepcopy_keys = lambda d, *kk: deepcopy({k: d[k] for k in kk})

repr_quote = partial(quote, safe=" /'\",;:[{}]=")
space_quote = partial(quote, safe=" /")


def split_version(version):
    """Convert strings like '4.0.3-rc1' into comparable tuples like (4, 0, 3, 'rc1')"""
    return tuple(int(p) if p.isdigit() else p for p in split('[-.]', version))


def random_unique_string(seed=""):
    return b64encode(uuid3(uuid4(), seed).bytes, b'_-').decode().rstrip("=")


@contextmanager
def pick_reachable_url(urls, name=None):
    """Iterate `urls` and get the first reachable URL"""
    def _pick():
        get_kws = dict(allow_redirects=True, stream=True)
        for url in urls:
            GLOpenAPILogger.debug(f"Trying URL: {url}")
            try:
                with request_get(url, **get_kws) as response:
                    if response.ok:
                        GLOpenAPILogger.debug(f"Hit URL: {url}")
                        return url
            except (URLError, OSError):
                GLOpenAPILogger.debug(f"Unreachable URL: {url}")
                continue
        else:
            if name:
                msg = f"No URLs are reachable for {name}: {urls}"
            else:
                msg = f"No URLs are reachable: {urls}"
            raise GLOpenAPIConfigurationException(msg)
    yield _pick()


def iterate_terminal_leaves(d, step_tracker=1, max_steps=256, isinstance=isinstance, dict=dict, enumerate=enumerate):
    """Descend into branches breadth-first and iterate terminal leaves; supports arbitrary values, does not support caching"""
    if step_tracker >= max_steps:
        msg = "Document branch exceeds nestedness threshold"
        raise GLOpenAPIConfigurationException(msg, max_steps=max_steps)
    elif isinstance(d, dict):
        for i, branch in enumerate(d.values(), start=1): # TODO need enumerate, or same level for immediate children?!
            yield from iterate_terminal_leaves(branch, step_tracker+i)
    else:
        yield d


def iterate_terminal_leaf_elements(d, iter_leaves=iterate_terminal_leaves, isinstance=isinstance, str=str, pattern=compile(r'\s*,\s*')):
    """Get terminal leaf of document and iterate filenames stored in leaf"""
    for value in iter_leaves(d):
        if isinstance(value, str):
            yield from pattern.split(value)


def flatten_all_keys(d, *, sep=".", head=()):
    """Recursively iterate nested dictionary `d`, yield flattened key sequences joined by `sep`"""
    for k, _d in d.items():
        if isinstance(_d, dict):
            yield from flatten_all_keys(_d, head=(*head, k))
        else:
            yield sep.join((*head, k))


def json_permissive_default(obj):
    """Serialize numpy entries as native types, sets as informative strings, other unserializable entries as their type names"""
    if isinstance(obj, NumpyGenericType):
        return obj.item()
    elif isinstance(obj, set):
        return f"<set>{list(obj)}"
    elif isinstance(obj, SRE_Pattern):
        return f"<SRE>{obj!r}"
    elif isinstance(obj, bytes):
        return str(obj)
    else:
        return str(type(obj))


def pdumps(obj, indent=4, default=json_permissive_default, *args, **kwargs):
    """Dump dictionaries and serialize nested objects in a generic way"""
    return dumps(obj, indent=indent, default=default, *args, **kwargs)


class ExceptionPropagatingThread(Thread):
    """Thread that raises errors in main thread on join()"""
    def run(self):
        self.exception = None
        try:
            super(ExceptionPropagatingThread, self).run()
        except Exception as exc:
            self.exception = exc
            raise
    def join(self):
        super(ExceptionPropagatingThread, self).join()
        if self.exception:
            raise self.exception
