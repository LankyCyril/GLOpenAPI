from pathlib import Path
from urllib.error import HTTPError
from flask import Response


def get(*, directory, filename, mimetype):
    """Pass contents of `filename` without the awfully designed `flask.url_for`"""
    parent = Path(filename).parent.parent.parent.parent
    def content():
        try:
            with open(parent / directory / filename, mode="rt") as handle:
                yield from handle
        except (FileNotFoundError, IOError, OSError):
            msg = "File not found"
            raise HTTPError(filename, 404, msg, hdrs=None, fp=None)
    return Response(content(), mimetype=mimetype)
