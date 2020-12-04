from flask import Response
from genefab3.exceptions import GeneLabFormatException


def render_raw(obj, context):
    """Display objects of various types in 'raw' format"""
    if isinstance(obj, bytes):
        return Response(obj, mimetype="application")
    elif isinstance(obj, str):
        return Response(obj, mimetype="text/plain")
    else:
        raise GeneLabFormatException(
            "Formatting of unsupported object type",
            object_type=type(obj).__name__, fmt="raw",
        )
