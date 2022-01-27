from pathlib import Path
from urllib.request import quote
from urllib.error import HTTPError
from flask import Response


def get(*, directory, filename, mode, mimetype):
    """Pass contents of `filename` without the awfully designed `flask.url_for`"""
    parent = Path(__file__).parent.parent.parent.parent
    safe_filename = quote(filename)
    def content():
        try:
            with open(parent / directory / safe_filename, mode=mode) as handle:
                yield from handle
        except (FileNotFoundError, IOError, OSError):
            msg = "File not found"
            raise HTTPError(safe_filename, 404, msg, hdrs=None, fp=None)
    return Response(content(), mimetype=mimetype)
