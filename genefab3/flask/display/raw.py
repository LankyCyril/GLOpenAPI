from flask import Response


def render_raw(obj, context):
    """Display objects of various types in 'raw' format"""
    if isinstance(obj, bytes):
        return Response(obj, mimetype="application")
    else:
        raise NotImplementedError("Display of {} with 'fmt=raw'".format(
            type(obj).__name__,
        ))
