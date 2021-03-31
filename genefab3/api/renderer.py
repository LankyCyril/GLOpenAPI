from flask import Response
from re import sub, MULTILINE
from pandas import Series, DataFrame
from json import dumps
from functools import wraps
from genefab3.api.parser import Context
from genefab3.common.exceptions import GeneFabException, GeneFabFormatException


class CacheableRenderer():
    """Renders objects returned by routes, and keeps them in LRU cache by `context.identity`"""
    TABLE_CSS = "table {table-layout: fixed; white-space: nowrap}"
 
    def __init__(self, sqlite_dbs):
        """Initialize object renderer and LRU cacher"""
        self.sqlite_dbs = sqlite_dbs
 
    def render_html(self, obj):
        """Display HTML code"""
        if isinstance(obj, str):
            return Response(obj, mimetype="text/html")
        else:
            return Response(obj.decode(), mimetype="text/html")
 
    def render_raw(self, obj):
        """Display objects of various types in 'raw' format"""
        if isinstance(obj, str):
            return Response(obj, mimetype="text/plain")
        else:
            return Response(obj, mimetype="application")
 
    def render_cls(self, obj, continuous=None, space_sub=lambda s: sub(r'\s', "", s)):
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
 
    def render_gct(obj):
        """Display presumed data dataframe in plaintext GCT format"""
        text = obj.to_csv(sep="\t", index=False, header=False)
        response = (
            "#1.2\n{}\t{}\n".format(obj.shape[0], obj.shape[1]-1) +
            "Name\tDescription\t" +
            "\t".join("/".join(levels) for levels in obj.columns[1:]) +
            "\n" + sub(r'^(.+?\t)', r'\1\1', text, flags=MULTILINE)
        )
        return Response(response, mimetype="text/plain")
 
    def render_json(self, obj, indent=None):
        """Display record in plaintext dump format"""
        return Response(dumps(obj, indent=indent), mimetype="text/plain")
 
    def render_dataframe(self, obj, fmt="tsv"):
        """Placeholder method""" # TODO
        return Response(
            f"<style>{self.TABLE_CSS}</style>" +
            obj.fillna("").to_html(index=False, col_space="1in"),
            mimetype="text/html",
        )
 
    def dispatch_renderer(self, obj, indent, fmt):
        """Render `obj` according to its type and passed kwargs"""
        _is = lambda t: isinstance(obj, t)
        _nlevels = getattr(getattr(obj, "columns", None), "nlevels", None)
        if obj is None:
            raise GeneFabException("No data")
        elif (fmt == "html") and _is((str, bytes)):
            return self.render_html(obj)
        elif (fmt == "raw") and _is((str, bytes)):
            return self.render_raw(obj)
        elif (fmt == "cls") and _is(DataFrame) and (_nlevels == 2):
            return self.render_cls(obj)
        elif (fmt == "gct") and _is(DataFrame) and (_nlevels == 3):
            return self.render_gct(obj)
        elif (fmt == "json") and _is((list, dict)):
            return self.render_json(obj, indent=indent)
        elif (fmt in {"tsv", "csv", "json", "browser"}) and _is(DataFrame):
            return self.render_dataframe(obj, fmt=fmt)
        else:
            raise GeneFabFormatException(
                "Formatting of unsupported object type",
                type=type(obj).__name__, nlevels=_nlevels, format=fmt,
            )
 
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
