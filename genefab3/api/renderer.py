from genefab3.api.renderers import SimpleRenderers, PlaintextDataFrameRenderers
from genefab3.api.renderers import BrowserDataFrameRenderers
from pandas import DataFrame
from functools import partial, wraps
from genefab3.api.parser import Context
from genefab3.common.exceptions import GeneFabException, GeneFabFormatException
from genefab3.common.exceptions import GeneFabConfigurationException


LevelCount = lambda x:x

TYPE_RENDERERS = {
    (str, bytes): {
        LevelCount(any): {
            "raw": SimpleRenderers.raw, "html": SimpleRenderers.html,
        },
    },
    (list, dict): {
        LevelCount(any): {
            "json": SimpleRenderers.json,
        },
    },
    DataFrame: {
        LevelCount(2): {
            "cls": PlaintextDataFrameRenderers.cls,
            "csv": partial(PlaintextDataFrameRenderers.xsv, sep=","),
            "tsv": partial(PlaintextDataFrameRenderers.xsv, sep="\t"),
            "json": PlaintextDataFrameRenderers.json,
            "browser": BrowserDataFrameRenderers.twolevel,
        },
        LevelCount(3): {
            "gct": PlaintextDataFrameRenderers.gct,
            "csv": partial(PlaintextDataFrameRenderers.xsv, sep=","),
            "tsv": partial(PlaintextDataFrameRenderers.xsv, sep="\t"),
            "json": PlaintextDataFrameRenderers.json,
            "browser": BrowserDataFrameRenderers.threelevel,
        },
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
            raise GeneFabException("No data")
        else:
            nlevels = getattr(getattr(obj, "columns", None), "nlevels", any)
        renderer_groups = [
            g for t, g in TYPE_RENDERERS.items() if isinstance(obj, t)
        ]
        if len(renderer_groups) == 0:
            renderer = None
        elif len(renderer_groups) == 1:
            renderer = renderer_groups[0].get(nlevels, {}).get(fmt, None)
        else:
            raise GeneFabConfigurationException(
                "Multiple TYPE_RENDERERS match object type",
                type=type(obj).__name__, nlevels=nlevels, format=fmt,
            )
        if renderer is None:
            raise GeneFabFormatException(
                "Formatting of unsupported object type",
                type=type(obj).__name__, nlevels=nlevels, format=fmt,
            )
        else:
            return renderer(obj, indent=indent)
 
    def __call__(self, method):
        """Render object returned from `method`, put in LRU cache by `context.identity`"""
        @wraps(method)
        def wrapper(*args, **kwargs):
            # TODO check cache
            context = Context()
            if context.kwargs["debug"] == "1":
                obj = context.__dict__
                indent, fmt = 4, "json"
            else:
                obj = method(*args, **kwargs)
                indent, fmt = None, context.kwargs.get(
                    "format", getattr(method, "fmt", "raw"),
                )
            response = self.dispatch_renderer(obj, indent, fmt)
            # TODO cache
            return response
        return wrapper
