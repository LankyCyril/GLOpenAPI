from flask import Response
from numpy import generic as NumpyGenericType
from json import dumps


def raw(obj, indent=None):
    """Display objects of various types in 'raw' format"""
    if isinstance(obj, str):
        return Response(obj, mimetype="text/plain")
    else:
        return Response(obj, mimetype="application")


def html(obj, indent=None):
    """Display HTML code"""
    if isinstance(obj, str):
        return Response(obj, mimetype="text/html")
    else:
        return Response(obj.decode(), mimetype="text/html")


def _json_default(o):
    """Serialize numpy entries as native types, other unserializable entries as their type names"""
    if isinstance(o, NumpyGenericType):
        return o.item()
    else:
        return str(type(o))


def json(obj, indent=None):
    """Display record in plaintext dump format"""
    content = dumps(obj, indent=indent, default=_json_default)
    return Response(content, mimetype="text/json")
