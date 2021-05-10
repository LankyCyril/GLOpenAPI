from json import dumps
from genefab3.common.utils import json_permissive_default


def raw(obj, context=None, indent=None):
    """Display objects of various types in 'raw' format"""
    content = obj
    mimetype = "text/plain" if isinstance(obj, str) else "application"
    return content, mimetype


def html(obj, context=None, indent=None):
    """Display HTML code""" # TODO stream
    content = obj.decode() if isinstance(obj, bytes) else obj
    return content, "text/html"


def json(obj, context=None, indent=None):
    """Display record in plaintext dump format"""
    content = dumps(obj, indent=indent, default=json_permissive_default)
    return content, "application/json"
