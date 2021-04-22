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
))


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
 
    def __init__(self, sqlite_dbs, flask_app):
        """Initialize object renderer and LRU cacher"""
        self.sqlite_dbs, self.flask_app = sqlite_dbs, flask_app
 
    def _infer_types(self, method):
        """Infer return types, default format, and cacheability based on type hints of method"""
        return_type = method.__annotations__.get("return")
        if return_type is None:
            return_types = set()
        elif getattr(return_type, "__origin__", None) is Union:
            return_types = set(return_type.__args__)
        else:
            return_types = {return_type}
        if return_types & {AnnotationDataFrame, DataDataFrame}:
            default_format, cacheable = "csv", True
        elif DataFrame in return_types:
            default_format, cacheable = "csv", False
        elif str in return_types:
            default_format, cacheable = "html", False
        else:
            default_format, cacheable = "raw", False
        return return_types, default_format, cacheable
 
    def _schemify(self, obj):
        """Reduce passed `obj` to representation of column types"""
        if isinstance(obj, DataFrame):
            return type(obj)(obj.dtypes.astype(str).to_frame().T)
        else:
            msg = "Argument 'schema' is not valid for requested data type"
            raise GeneFabFormatException(msg, type=type(obj).__name__)
 
    def dispatch_renderer(self, obj, context, default_format, indent=None):
        """Render `obj` according to its type and passed kwargs"""
        if obj is None:
            raise GeneFabConfigurationException("Route returned no object")
        else:
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
 
    def __call__(self, method):
        """Render object returned from `method`, put in LRU cache by `context.identity`"""
        @wraps(method)
        def wrapper(*args, **kwargs):
            context = Context(self.flask_app)
            if context.debug == "1":
                obj = deepcopy(context.__dict__)
                context.format = "json"
                _kw = dict(context=context, indent=4, default_format="json")
                response = self.dispatch_renderer(obj, **_kw)
            else:
                return_types, default_format, cached = self._infer_types(method)
                if cached:
                    response_cache = ResponseCache(self.sqlite_dbs)
                    response = response_cache.get(context)
                else:
                    response_cache, response = None, None
                if response is None:
                    obj = method(*args, context=context, **kwargs)
                    if context.schema == "1":
                        obj = self._schemify(obj)
                    _kw = dict(context=context, default_format=default_format)
                    response = self.dispatch_renderer(obj, **_kw)
                    if response.status_code == 200:
                        if response_cache is not None:
                            response_cache.put(context, obj, response)
            return response
        return wrapper
