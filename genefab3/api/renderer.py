from flask import Response, request
from functools import wraps
from genefab3.api.parser import parse_request
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
            context = parse_request(request)
            ...
            from json import dumps
            print(dumps(context.__dict__, indent=4))
            ...
            obj = method(*args, **kwargs)
            if isinstance(obj, (list, dict)):
                return self.render_json(obj)
            elif isinstance(obj, DataFrame):
                return self.render_dataframe(obj)
            else:
                return Response(obj)
        return wrapper
