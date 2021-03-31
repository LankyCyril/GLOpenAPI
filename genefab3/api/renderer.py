from flask import Response
from re import sub, MULTILINE
from pandas import Series, DataFrame
from json import dumps
from functools import partial, wraps
from memoized_property import memoized_property
from genefab3.api.parser import Context
from genefab3.common.exceptions import GeneFabException, GeneFabFormatException
from genefab3.common.exceptions import GeneFabConfigurationException
from genefab3.common.utils import JSONByteEncoder


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
 
    def __init__(self, sqlite_dbs):
        """Initialize object renderer and LRU cacher"""
        self.sqlite_dbs = sqlite_dbs
 
    @memoized_property
    def TYPE_RENDERERS(self):
        """Dispatches renderer method based on `obj` type and properties; type->nlevels->format"""
        LevelCount = lambda x:x
        return {
            (str, bytes): {
                LevelCount(any): {
                    "raw": self.render_raw,
                    "html": self.render_html,
                },
            },
            (list, dict): {
                LevelCount(any): {
                    "json": self.render_json,
                },
            },
            DataFrame: {
                LevelCount(2): {
                    "cls": self.render_cls,
                    "csv": partial(self.render_plaintext_dataframe, sep=","),
                    "tsv": partial(self.render_plaintext_dataframe, sep="\t"),
                    "json": partial(self.render_plaintext_dataframe, json=True),
                    "browser": self.render_browser_twolevel,
                },
                LevelCount(3): {
                    "gct": self.render_gct,
                    "csv": partial(self.render_plaintext_dataframe, sep=","),
                    "tsv": partial(self.render_plaintext_dataframe, sep="\t"),
                    "json": partial(self.render_plaintext_dataframe, json=True),
                    "browser": self.render_browser_threelevel,
                },
            },
        }
 
    def dispatch_renderer(self, obj, indent, fmt):
        """Render `obj` according to its type and passed kwargs"""
        if obj is None:
            raise GeneFabException("No data")
        else:
            nlevels = getattr(getattr(obj, "columns", None), "nlevels", any)
        renderer_groups = [
            g for t, g in self.TYPE_RENDERERS.items() if isinstance(obj, t)
        ]
        if len(renderer_groups) == 0:
            renderer = None
        elif len(renderer_groups) == 1:
            renderer = renderer_groups[0].get(nlevels, {}).get(fmt, None)
        else:
            raise GeneFabConfigurationException("TODO") # TODO
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
 
    def render_raw(self, obj, indent=None):
        """Display objects of various types in 'raw' format"""
        if isinstance(obj, str):
            return Response(obj, mimetype="text/plain")
        else:
            return Response(obj, mimetype="application")
 
    def render_html(self, obj, indent=None):
        """Display HTML code"""
        if isinstance(obj, str):
            return Response(obj, mimetype="text/html")
        else:
            return Response(obj.decode(), mimetype="text/html")
 
    def render_json(self, obj, indent=None):
        """Display record in plaintext dump format"""
        return Response(dumps(obj, indent=indent), mimetype="text/json")
 
    def render_cls(self, obj, continuous=None, space_sub=lambda s: sub(r'\s', "", s), indent=None):
        """Display presumed annotation/factor dataframe in plaintext CLS format"""
        columns = [(l0, l1) for (l0, l1) in obj.columns if l0 != "info"]
        if len(columns) != 1:
            m = "Exactly one metadata field must be requested"
            raise GeneFabFormatException(m, columns=columns, format="cls")
        target, sample_count = columns[0], obj.shape[0]
        if (continuous is None) or (continuous is True):
            try:
                _data = [["#numeric"], ["#"+target], obj[target].astype(float)]
            except ValueError:
                if continuous is True:
                    m = "Cannot represent target annotation as continuous"
                    raise GeneFabFormatException(m, target=target, format="cls")
                else:
                    continuous = False
        if continuous is False:
            _sub, classes = space_sub or (lambda s: s), obj[target].unique()
            class2id = Series(index=classes, data=range(len(classes)))
            _data = [
                [sample_count, len(classes), 1],
                ["# "+_sub(classes[0])] + [_sub(c) for c in classes[1:]],
                [class2id[v] for v in obj[target]]
            ]
        response = "\n".join(["\t".join([str(f) for f in fs]) for fs in _data])
        return Response(response, mimetype="text/plain")
 
    def render_gct(obj, indent=None):
        """Display presumed data dataframe in plaintext GCT format"""
        text = obj.to_csv(sep="\t", index=False, header=False)
        response = (
            "#1.2\n{}\t{}\n".format(obj.shape[0], obj.shape[1]-1) +
            "Name\tDescription\t" +
            "\t".join("/".join(levels) for levels in obj.columns[1:]) +
            "\n" + sub(r'^(.+?\t)', r'\1\1', text, flags=MULTILINE)
        )
        return Response(response, mimetype="text/plain")
 
    def render_plaintext_dataframe(self, obj, sep=",", json=False, indent=None):
        """Display dataframe in plaintext `sep`-separated format or as JSON"""
        if json:
            raw_json = {
                "columns": obj.columns.tolist(), "data": obj.values.tolist(),
            }
            return Response(
                dumps(raw_json, indent=indent, cls=JSONByteEncoder),
                mimetype="text/json",
            )
        else:
            _kws = dict(sep=sep, index=False, header=False, na_rep="NA")
            header = sub(r'^', "#", sub(r'\n(.)', r'\n#\1',
                obj.columns.to_frame().T.to_csv(**_kws),
            ))
            return Response(header + obj.to_csv(**_kws), mimetype="text/plain")
 
    def render_browser_twolevel(self, obj, indent=None):
        """Placeholder method""" # TODO
        TABLE_CSS = "table {table-layout: fixed; white-space: nowrap}"
        return Response(
            f"<style>{TABLE_CSS}</style>" +
            obj.fillna("").to_html(index=False, col_space="1in"),
            mimetype="text/html",
        )
 
    def render_browser_threelevel(self, obj, indent=None):
        """Placeholder method""" # TODO
        return None
