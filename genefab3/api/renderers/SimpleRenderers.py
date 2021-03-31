from flask import Response
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


def json(obj, indent=None):
    """Display record in plaintext dump format"""
    return Response(dumps(obj, indent=indent), mimetype="text/json")
