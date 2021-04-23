from flask import Response
from json import dumps
from genefab3.common.utils import json_permissive_default


def raw(obj, context=None, indent=None):
    """Display objects of various types in 'raw' format"""
    mimetype = "text/plain" if isinstance(obj, str) else "application"
    return Response(obj, mimetype=mimetype)


def html(obj, context=None, indent=None):
    """Display HTML code"""
    content = obj.decode() if isinstance(obj, bytes) else obj
    return Response(content, mimetype="text/html")


def json(obj, context=None, indent=None):
    """Display record in plaintext dump format"""
    content = dumps(obj, indent=indent, default=json_permissive_default)
    return Response(content, mimetype="application/json")
