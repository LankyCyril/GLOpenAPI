from flask import Response
from functools import wraps
from genefab3.api.parser import Context
from genefab3.common.exceptions import GeneFabException, GeneFabFormatException
from pandas import DataFrame


class CacheableRenderer():
    """ """ # TODO fill in docstring
    TABLE_CSS = "table {table-layout: fixed; white-space: nowrap}"
 
    def __init__(self, sqlite_dbs):
        """ """ # TODO fill in docstring
        self.sqlite_dbs = sqlite_dbs
 
    def render_json(self, obj):
        """ """ # TODO fill in docstring
        return Response(obj) # TODO
 
    def render_dataframe(self, obj):
        """Placeholder method""" # TODO
        return Response(
            f"<style>{self.TABLE_CSS}</style>" +
            obj.fillna("").to_html(index=False, col_space="1in"),
            mimetype="text/html",
        )
 
    def __call__(self, method):
        """Placeholder methods so far""" # TODO
        @wraps(method)
        def wrapper(*args, **kwargs):
            obj, _format = method(*args, **kwargs), Context().kwargs["format"]
            if obj is None:
                raise GeneFabException("No data")
            elif _format == "raw":
                return self.render_raw(obj) # TODO
            elif _format == "cls":
                return self.render_cls(obj) # TODO
            elif _format == "gct":
                return self.render_gct(obj) # TODO
            elif isinstance(obj, (list, dict)):
                return self.render_json(obj)
            elif isinstance(obj, DataFrame):
                return self.render_dataframe(obj)
            else:
                raise GeneFabFormatException(
                    "Formatting of unsupported object type",
                    type=type(obj).__name__, format=_format,
                )
        return wrapper
