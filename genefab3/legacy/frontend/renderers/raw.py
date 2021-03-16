from flask import Response
from genefab3.common.exceptions import GeneFabFormatException


def render_raw(obj, context):
    """Display objects of various types in 'raw' format"""
    if isinstance(obj, bytes):
        return Response(obj, mimetype="application")
    elif isinstance(obj, str):
        return Response(obj, mimetype="text/plain")
    else:
        raise GeneFabFormatException(
            "Formatting of unsupported object type",
            object_type=type(obj).__name__, format="raw",
        )
