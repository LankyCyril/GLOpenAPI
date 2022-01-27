from collections import OrderedDict
from flask import Response
from glopenapi.api.renderers.types import StreamedAnnotationTable
from glopenapi.api.renderers.types import StreamedSchema, StreamedDataTable
from glopenapi.api.renderers.types import StreamedString
from glopenapi.api.renderers.types import StreamedAnnotationValueCounts
from glopenapi.api.renderers import PlaintextStreamedTableRenderers
from glopenapi.api.renderers import BrowserStreamedTableRenderers
from glopenapi.api.renderers import PlaintextStreamedJSONRenderers
from glopenapi.api.renderers import SimpleRenderers
from glopenapi.common.exceptions import GLOpenAPIFormatException
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from glopenapi.db.sql.response_cache import ResponseCache
from glopenapi.common.utils import ExceptionPropagatingThread
from functools import wraps
from copy import deepcopy
from glopenapi.api.types import ResponseContainer


TYPE_RENDERERS = OrderedDict((
    (Response, {
        "raw": lambda obj, *a, **k: (obj, "application"),
    }),
    (StreamedAnnotationTable, {
        "cls": PlaintextStreamedTableRenderers.cls,
        "csv": PlaintextStreamedTableRenderers.csv,
        "tsv": PlaintextStreamedTableRenderers.tsv,
        "json": PlaintextStreamedTableRenderers.json,
        "browser": BrowserStreamedTableRenderers.html,
    }),
    (StreamedDataTable, {
        "gct": PlaintextStreamedTableRenderers.gct,
        "csv": PlaintextStreamedTableRenderers.csv,
        "tsv": PlaintextStreamedTableRenderers.tsv,
        "json": PlaintextStreamedTableRenderers.json,
        "browser": BrowserStreamedTableRenderers.html,
    }),
    (StreamedSchema, {
        "csv": PlaintextStreamedTableRenderers.csv,
        "tsv": PlaintextStreamedTableRenderers.tsv,
        "json": PlaintextStreamedTableRenderers.json,
        "browser": BrowserStreamedTableRenderers.html,
    }),
    (StreamedAnnotationValueCounts, {
        "json": PlaintextStreamedJSONRenderers.json,
    }),
    ((StreamedString, str, bytes), {
        "raw": SimpleRenderers.raw,
        "html": SimpleRenderers.html,
    }),
    ((list, dict), {
        "json": SimpleRenderers.json, # TODO: use PlaintextStreamedJSONRenderers
    }),
))


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
 
    def __init__(self, *, sqlite_dbs, get_context, cleanup):
        """Initialize object renderer and LRU cacher"""
        self.sqlite_dbs = sqlite_dbs
        self.get_context = get_context
        self.cleanup = cleanup
 
    def dispatch_renderer(self, obj, context, default_format, indent=None):
        """Render `obj` according to its type and passed kwargs: pass through content and mimetype"""
        for types, fmt_to_renderer in TYPE_RENDERERS.items():
            if isinstance(obj, types):
                if context.format is None:
                    renderer = fmt_to_renderer[default_format]
                elif context.format in fmt_to_renderer:
                    renderer = fmt_to_renderer[context.format]
                else:
                    raise GLOpenAPIFormatException(
                        "Requested format not valid for requested data",
                        type=type(obj).__name__, format=context.format,
                        default_format=default_format,
                    )
                return renderer(obj, context, indent=indent)
        else:
            msg = "Route returned unsupported object"
            raise GLOpenAPIConfigurationException(msg, type=type(obj).__name__)
 
    def _get_response_container_via_cache(self, context, method, args, kwargs):
        """Render object returned from `method`, put in LRU cache by `context.identity`"""
        response_cache = ResponseCache(self.sqlite_dbs)
        response_container = response_cache.get(context)
        if response_container.empty:
            def _call_and_cache():
                obj = method(*args, context=context, **kwargs)
                try:
                    obj = obj.schema if (context.schema == "1") else obj
                except AttributeError:
                    msg = "'schema=1' is not valid for requested data"
                    _type = type(obj).__name__
                    raise GLOpenAPIFormatException(msg, type=_type)
                default_format = getattr(obj, "default_format", "raw")
                content, mimetype = self.dispatch_renderer(
                    obj, context=context, default_format=default_format,
                )
                response_container.update(content, mimetype, obj)
                if getattr(obj, "cacheable", None) is True:
                    if response_cache is not None:
                        response_cache.put(response_container, context)
            _thread = ExceptionPropagatingThread(target=_call_and_cache)
            _thread.start() # will complete even after timeout errors
            _thread.join() # will fill container if does not time out
        return response_container
 
    def __call__(self, method):
        """Handle object returned from `method`, return either debug information or rendered object (optionally cached)"""
        @wraps(method)
        def wrapper(*args, **kwargs):
            context = self.get_context()
            try:
                if context.debug == "1":
                    obj, context.format = deepcopy(context.__dict__), "json"
                    content, mimetype = self.dispatch_renderer(
                        obj, context=context, indent=4, default_format="json",
                    )
                    response_container = ResponseContainer(
                        content, mimetype, obj,
                    )
                else:
                    response_container = self._get_response_container_via_cache(
                        context, method, args, kwargs,
                    )
            finally:
                self.cleanup()
            return response_container.make_response()
        return wrapper
