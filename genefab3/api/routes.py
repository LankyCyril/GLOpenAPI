from functools import wraps, partial
from genefab3.common.exceptions import GeneFabException
from genefab3.api import views


class Routes():
 
    def __init__(self, mongo_collections):
        self.mongo_collections = mongo_collections
 
    def _as_endpoint(method, endpoint=None):
        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)
        if endpoint:
            wrapper.endpoint = endpoint
        elif hasattr(method, "__name__") and isinstance(method.__name__, str):
            wrapper.endpoint = "/" + method.__name__ + "/"
        return wrapper
 
    def items(self):
        for name in dir(self):
            method = getattr(self, name)
            if isinstance(getattr(method, "endpoint", None), str):
                yield method.endpoint, method
 
    @partial(_as_endpoint, endpoint="/favicon.<imgtype>")
    def favicon(self, imgtype):
        return ""
 
    @partial(_as_endpoint, endpoint="/debug/")
    def debug(self):
        return "OK"
 
    @partial(_as_endpoint, endpoint="/debug/error/")
    def debug_error(self):
        raise GeneFabException("Generic error test")
        return "OK (raised exception)"
 
    @partial(_as_endpoint, endpoint="/")
    def root(self):
        return "Hello space"
 
    @_as_endpoint
    def status(self):
        return views.status.get(self.mongo_collections)
