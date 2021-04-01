from genefab3.api.renderers import SimpleRenderers, PlaintextDataFrameRenderers
from genefab3.api.renderers import BrowserDataFrameRenderers
from pandas import DataFrame
from functools import wraps
from genefab3.common.utils import match_mapping
from operator import eq
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.exceptions import GeneFabFormatException
from genefab3.api.parser import Context


LevelCount = lambda *a:a

TYPE_RENDERERS = {
    DataFrame: {
        "cls": {LevelCount(2): PlaintextDataFrameRenderers.cls},
        "gct": {LevelCount(3): PlaintextDataFrameRenderers.gct},
        "csv": {LevelCount(2,3): PlaintextDataFrameRenderers.csv},
        "tsv": {LevelCount(2,3): PlaintextDataFrameRenderers.tsv},
        "json": {LevelCount(2,3): PlaintextDataFrameRenderers.json},
        "browser": {
            LevelCount(2): BrowserDataFrameRenderers.twolevel,
            LevelCount(3): BrowserDataFrameRenderers.threelevel,
        },
    },
    (str, bytes): {
        "raw": {LevelCount(None): SimpleRenderers.raw},
        "html": {LevelCount(None): SimpleRenderers.html},
    },
    (list, dict): {
        "json": {LevelCount(None): SimpleRenderers.json},
    },
}


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
 
    def __init__(self, sqlite_dbs):
        """Initialize object renderer and LRU cacher"""
        self.sqlite_dbs = sqlite_dbs
 
    def dispatch_renderer(self, obj, indent, fmt):
        """Render `obj` according to its type and passed kwargs"""
        if obj is None:
            raise GeneFabConfigurationException("Route returned no data")
        nlevels = getattr(getattr(obj, "columns", None), "nlevels", None)
        matchers = (isinstance, obj), (eq, fmt), (lambda a, b: a in b, nlevels)
        _error_kws = dict(type=type(obj).__name__, nlevels=nlevels, format=fmt)
        try:
            renderer = match_mapping(TYPE_RENDERERS, matchers)
        except KeyError:
            msg = "Formatting of unsupported object type"
            raise GeneFabFormatException(msg, **_error_kws)
        except ValueError:
            msg = "Multiple TYPE_RENDERERS match object type"
            raise GeneFabConfigurationException(msg, **_error_kws)
        else:
            return renderer(obj, indent=indent)
 
    def __call__(self, method):
        """Render object returned from `method`, put in LRU cache by `context.identity`"""
        @wraps(method)
        def wrapper(*args, **kwargs):
            context = Context()
            # TODO check cache based on context.identity
            if context.kwargs["debug"] == "1":
                response = self.dispatch_renderer(context.__dict__, 4, "json")
            else:
                response = self.dispatch_renderer(
                    method(*args, **kwargs), None,
                    context.kwargs.get("format", getattr(method, "fmt", "raw")),
                )
            # TODO cache
            return response
        return wrapper
