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
            "Display of {} with 'fmt=raw'".format(type(obj).__name__),
        )
