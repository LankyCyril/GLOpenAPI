from functools import wraps
from genefab3.common.exceptions import GeneFabJSONException
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
        return f"This is Patrick"
 
    @_as_endpoint
    def mayonnaise(self):
        return f"This is an instrument"
 
    @_as_endpoint
    def error(self):
        raise GeneFabJSONException("Testing testing", "GLDS-1000", baba="booey")
 
    @_as_endpoint
    def warning(self):
        GeneFabLogger().warning("Ye be warn'd again")
        return "Check yo warnings yo"
