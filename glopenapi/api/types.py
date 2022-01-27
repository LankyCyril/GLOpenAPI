from functools import wraps
from flask import Response
from collections.abc import Callable
from glopenapi.common.exceptions import GLOpenAPIConfigurationException


class Routes():
    """Base class for registered endpoints"""
 
    def __init__(self, glopenapi_client):
        self.glopenapi_client = glopenapi_client
 
    def register_endpoint(endpoint=None):
        """Decorator that adds `endpoint` and `fmt` attributes to class method"""
        def outer(method):
            @wraps(method)
            def inner(*args, **kwargs):
                return method(*args, **kwargs)
            if endpoint:
                inner.endpoint = endpoint
            elif hasattr(method, "__name__"):
                if isinstance(method.__name__, str):
                    inner.endpoint = "/" + method.__name__ + "/"
            return inner
        return outer
 
    def items(self):
        """Iterate over methods of `Routes` object that have `endpoint` attribute"""
        for name in dir(self):
            method = getattr(self, name)
            if isinstance(getattr(method, "endpoint", None), str):
                yield method.endpoint, method


class ResponseContainer():
    """Holds content (bytes, strings, streamer function, or Response), mimetype, and originating object"""
    def update(self, content=None, mimetype=None, obj=None):
        self.content, self.mimetype, self.obj = content, mimetype, obj
    def __init__(self, content=None, mimetype=None, obj=None):
        self.update(content, mimetype, obj)
    @property
    def empty(self):
        return self.content is None
    def make_response(self):
        if isinstance(self.content, Response):
            return self.content
        elif isinstance(self.content, Callable):
            return Response(self.content(), mimetype=self.mimetype)
        elif self.content is not None:
            return Response(self.content, mimetype=self.mimetype)
        else:
            msg = "Route returned no response"
            raise GLOpenAPIConfigurationException(msg)
