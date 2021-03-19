from functools import wraps
from genefab3.common.logger import GeneFabLogger


class Routes():
 
    def _as_endpoint(method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            return method(*args, **kwargs)
        if hasattr(method, "__name__") and isinstance(method.__name__, str):
            wrapper.endpoint = "/" + method.__name__ + "/"
        return wrapper
 
    def items(self):
        for name in dir(self):
            method = getattr(self, name)
            if hasattr(method, "endpoint"):
                yield method.endpoint, method
 
    @_as_endpoint
    def status(self):
        GeneFabLogger().warning("/status/ not implemented")
        return "None"
