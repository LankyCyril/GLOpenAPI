from flask import Response
from numpy import generic as NumpyGenericType
from json import dumps


def raw(obj, context=None, indent=None):
    """Display objects of various types in 'raw' format"""
    mimetype = "text/plain" if isinstance(obj, str) else "application"
    return Response(obj, mimetype=mimetype)


def html(obj, context=None, indent=None):
    """Display HTML code"""
    content = obj.decode() if isinstance(obj, bytes) else obj
    return Response(content, mimetype="text/html")


def _json_default(o):
    """Serialize numpy entries as native types, sets as informative strings, other unserializable entries as their type names"""
    if isinstance(o, NumpyGenericType):
        return o.item()
    elif isinstance(o, set):
        return f"<set>{list(o)}"
    else:
        return str(type(o))


def json(obj, context=None, indent=None):
    """Display record in plaintext dump format"""
    content = dumps(obj, indent=indent, default=_json_default)
    return Response(content, mimetype="application/json")
