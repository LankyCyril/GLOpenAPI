#!/usr/bin/env python
from flask import Flask, request
from flask_compress import Compress
from genefab3.config import FLASK_DEBUG_MARKERS, COMPRESSIBLE_MIMETYPES
from os import environ
from genefab3.exceptions import traceback_printer, exception_catcher
from genefab3.readme import readme


app = Flask("genefab3")
COMPRESS_MIMETYPES = COMPRESSIBLE_MIMETYPES
Compress(app)


if environ.get("FLASK_ENV", None) in FLASK_DEBUG_MARKERS:
    traceback_printer = app.errorhandler(Exception)(traceback_printer)
else:
    exception_catcher = app.errorhandler(Exception)(exception_catcher)


@app.route("/favicon.<imgtype>")
def favicon(imgtype):
    """Catch request for favicons"""
    return ""


@app.route("/", methods=["GET"])
def landing_page():
    """Hello, Space!"""
    return readme(url_root=request.url_root.rstrip("/"))
