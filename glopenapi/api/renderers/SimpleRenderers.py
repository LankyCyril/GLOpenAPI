from json import dumps
from collections.abc import Callable
from glopenapi.api.renderers.types import StreamedString
from glopenapi.common.utils import json_permissive_default


def raw(obj, context=None, indent=None):
    """Display objects of various types in 'raw' format"""
    content = obj
    mimetype = "text/plain" if isinstance(obj, str) else "application"
    return content, mimetype


def javascript(obj, context=None, indent=None):
    """Display javascript"""
    content = StreamedString(obj, default_format="javascript")
    # TODO: merge with libs_js in routes and/or
    # glopenapi.api.renderers.types.StreamedString
    return content, "application/javascript"


def html(obj, context=None, indent=None):
    """Display HTML code"""
    if isinstance(obj, bytes):
        content = obj.decode()
    elif isinstance(obj, Callable):
        content = StreamedString(obj, default_format="html")
    else:
        content = obj
    # TODO: check how this is used and whether it can/should be merged with
    # glopenapi.api.renderers.types.StreamedString
    return content, "text/html"


def json(obj, context=None, indent=None):
    """Display record in plaintext dump format"""
    content = dumps(obj, indent=indent, default=json_permissive_default)
    return content, "application/json"
