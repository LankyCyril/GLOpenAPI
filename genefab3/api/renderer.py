from collections import OrderedDict
from flask import Response
from genefab3.common.types import StreamedAnnotationTable, StreamedDataTable
from genefab3.common.types import StreamedSchema
from genefab3.api.renderers import PlaintextStreamedTableRenderers
from genefab3.api.renderers import BrowserStreamedTableRenderers
from genefab3.api.renderers import SimpleRenderers
from genefab3.common.types import StringIterator
from genefab3.common.exceptions import GeneFabFormatException
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.utils import ExceptionPropagatingThread
from functools import wraps
from genefab3.api.parser import Context
from copy import deepcopy
from genefab3.common.types import ResponseContainer
from genefab3.db.sql.response_cache import ResponseCache


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
    ((StringIterator, str, bytes), {
        "raw": SimpleRenderers.raw,
        "html": SimpleRenderers.html,
    }),
    ((list, dict), {
        "json": SimpleRenderers.json,
    }),
))


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
 
    def __init__(self, genefab3_client):
        """Initialize object renderer and LRU cacher"""
        self.genefab3_client = genefab3_client
 
    def dispatch_renderer(self, obj, context, default_format, indent=None):
        """Render `obj` according to its type and passed kwargs: pass through content and mimetype"""
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
            context = Context(self.genefab3_client.flask_app)
            if context.debug == "1":
                obj, context.format = deepcopy(context.__dict__), "json"
                content, mimetype = self.dispatch_renderer(
                    obj, context=context, indent=4, default_format="json",
                )
                response_container = ResponseContainer(content, mimetype)
            else:
                response_cache = ResponseCache(self.genefab3_client.sqlite_dbs)
                response_container = response_cache.get(context)
                if response_container.empty:
                    def _call_and_cache():
                        obj = method(*args, context=context, **kwargs)
                        try:
                            obj = obj.schema if (context.schema == "1") else obj
                        except AttributeError:
                            msg = "'schema=1' is not valid for requested data"
                            _type = type(obj).__name__
                            raise GeneFabFormatException(msg, type=_type)
                        default_format = getattr(obj, "default_format", "raw")
                        content, mimetype = self.dispatch_renderer(
                            obj, context=context, default_format=default_format,
                        )
                        response_container.update(content, mimetype)
                        if getattr(obj, "cacheable", None) is True:
                            if response_cache is not None: # TODO
                                pass #response_cache.put(context, response_container)
                    _thread = ExceptionPropagatingThread(target=_call_and_cache)
                    _thread.start() # will complete even after timeout errors
                    _thread.join() # will fill container if does not time out
            if not self.genefab3_client.cacher_thread.isAlive():
                self.genefab3_client.mongo_client.close()
            return response_container.make_response()
        return wrapper
