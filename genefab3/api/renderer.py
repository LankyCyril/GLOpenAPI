from collections import OrderedDict
from flask import Response
from genefab3.api.renderers import SimpleRenderers, PlaintextDataFrameRenderers
from genefab3.api.renderers import BrowserDataFrameRenderers
from genefab3.common.types import AnnotationDataFrame, DataDataFrame
from pandas import DataFrame
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.exceptions import GeneFabFormatException
from typing import Union
from functools import wraps
from genefab3.api.parser import Context
from copy import deepcopy
from genefab3.db.sql.response_cache import ResponseCache
from genefab3.common.utils import ExceptionPropagatingThread


from genefab3.common.types import StreamedAnnotationTable
from genefab3.api.renderers import PlaintextStreamedTableRenderers
from genefab3.api.renderers import BrowserStreamedTableRenderers


TYPE_RENDERERS = OrderedDict((
    (Response, {
        "raw": lambda obj, *a, **k: obj,
    }),
    ((str, bytes), {
        "raw": SimpleRenderers.raw,
        "html": SimpleRenderers.html,
    }),
    ((list, dict), {
        "json": SimpleRenderers.json,
    }),
    (AnnotationDataFrame, {
        "cls": PlaintextDataFrameRenderers.cls,
        "csv": PlaintextDataFrameRenderers.csv,
        "tsv": PlaintextDataFrameRenderers.tsv,
        "json": PlaintextDataFrameRenderers.json,
        "browser": BrowserDataFrameRenderers.twolevel,
    }),
    (DataDataFrame, {
        "gct": PlaintextDataFrameRenderers.gct,
        "csv": PlaintextDataFrameRenderers.csv,
        "tsv": PlaintextDataFrameRenderers.tsv,
        "json": PlaintextDataFrameRenderers.json,
        "browser": BrowserDataFrameRenderers.threelevel,
    }),
    (DataFrame, {
        "csv": PlaintextDataFrameRenderers.csv,
        "tsv": PlaintextDataFrameRenderers.tsv,
        "json": PlaintextDataFrameRenderers.json,
        "browser": BrowserDataFrameRenderers.twolevel,
    }),
    (StreamedAnnotationTable, {
        "csv": PlaintextStreamedTableRenderers.csv,
        "tsv": PlaintextStreamedTableRenderers.tsv,
        "json": PlaintextStreamedTableRenderers.json,
        "browser": BrowserStreamedTableRenderers.twolevel,
    }),
))


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
 
    def __init__(self, genefab3_client):
        """Initialize object renderer and LRU cacher"""
        self.genefab3_client = genefab3_client
        self.sqlite_dbs = genefab3_client.sqlite_dbs
        self.flask_app = genefab3_client.flask_app
 
    def _infer_types(self, method):
        """Infer return types, default format, and cacheability based on type hints of method"""
        return_type = method.__annotations__.get("return")
        if return_type is None:
            return_types = set()
        elif getattr(return_type, "__origin__", None) is Union:
            return_types = set(return_type.__args__)
        else:
            return_types = {return_type}
        if return_types & {AnnotationDataFrame, DataDataFrame, StreamedAnnotationTable}:
            default_format, cacheable = "csv", True
        elif DataFrame in return_types:
            default_format, cacheable = "csv", False
        elif str in return_types:
            default_format, cacheable = "html", False
        else:
            default_format, cacheable = "raw", False
        return return_types, default_format, cacheable
 
    def dispatch_renderer(self, obj, context, default_format, indent=None):
        """Render `obj` according to its type and passed kwargs"""
        for types, fmt_to_renderer in TYPE_RENDERERS.items():
            if isinstance(obj, types):
                if context.format is None:
                    renderer = fmt_to_renderer[default_format]
                elif context.format in fmt_to_renderer:
                    renderer = fmt_to_renderer[context.format]
                else:
                    raise GeneFabFormatException(
                        "Requested format not valid for requested data",
                        type=type(obj).__name__, format=context.format,
                        default_format=default_format,
                    )
                return renderer(obj, context, indent=indent)
        else:
            msg = "Route returned unsupported object"
            raise GeneFabConfigurationException(msg, type=type(obj).__name__)
 
    def __call__(self, method):
        """Render object returned from `method`, put in LRU cache by `context.identity`"""
        @wraps(method)
        def wrapper(*args, **kwargs):
            context = Context(self.flask_app)
            if context.debug == "1":
                obj = deepcopy(context.__dict__)
                context.format = "json"
                _kw = dict(context=context, indent=4, default_format="json")
                container = [self.dispatch_renderer(obj, **_kw)]
            else:
                return_types, default_format, cached = self._infer_types(method)
                if cached:
                    response_cache = ResponseCache(self.sqlite_dbs)
                    response = response_cache.get(context)
                    container = [response] if response else []
                else:
                    response_cache, container = None, []
                if not container:
                    def _call_and_cache():
                        obj = method(*args, context=context, **kwargs)
                        if context.schema == "1":
                            try:
                                obj = obj.schema
                            except AttributeError:
                                m = "'schema=1' is not valid for requested data"
                                _kw = dict(type=type(obj).__name__)
                                raise GeneFabFormatException(m, **_kw)
                        k = dict(context=context, default_format=default_format)
                        container.append(self.dispatch_renderer(obj, **k))
                        if container[0].status_code == 200:
                            if response_cache is not None:
                                pass # TODO need to put result post-hoc?
                                #response_cache.put(context, obj, container[0])
                    _thread = ExceptionPropagatingThread(target=_call_and_cache)
                    _thread.start() # will complete even after timeout errors
                    _thread.join() # will fill container if does not time out
            if not self.genefab3_client.cacher_thread.isAlive():
                self.genefab3_client.mongo_client.close()
            return (container or [None])[0]
        return wrapper
