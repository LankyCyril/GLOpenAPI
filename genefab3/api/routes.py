from functools import wraps
from genefab3.common.exceptions import GeneFabException
from genefab3.api import views


class Routes():
 
    def __init__(self, mongo_collections):
        self.mongo_collections = mongo_collections
 
    def as_endpoint(*, endpoint=None, fmt="tsv"):
        def outer(method):
            @wraps(method)
            def inner(*args, **kwargs):
                return method(*args, **kwargs)
            if endpoint:
                inner.endpoint = endpoint
            elif hasattr(method, "__name__"):
                if isinstance(method.__name__, str):
                    inner.endpoint = "/" + method.__name__ + "/"
            inner.fmt = fmt
            return inner
        return outer
 
    def items(self):
        for name in dir(self):
            method = getattr(self, name)
            if isinstance(getattr(method, "endpoint", None), str):
                yield method.endpoint, method
 
    @as_endpoint(endpoint="/favicon.<imgtype>", fmt="raw")
    def favicon(self, imgtype):
        return ""
 
    @as_endpoint(endpoint="/debug/", fmt="raw")
    def debug(self):
        return "OK"
 
    @as_endpoint(endpoint="/debug/error/", fmt="raw")
    def debug_error(self):
        raise GeneFabException("Generic error test")
        return "OK (raised exception)"
 
    @as_endpoint(endpoint="/", fmt="html")
    def root(self):
        return "Hello space"
 
    @as_endpoint()
    def status(self):
        return views.status.get(self.mongo_collections)
