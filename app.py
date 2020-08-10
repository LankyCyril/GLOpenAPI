#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.exceptions import GeneLabDatabaseException
from genefab3.config import MONGO_DB_NAME, DEBUG_MARKERS, COMPRESSIBLE_MIMETYPES
from flask import Flask, request
from flask_compress import Compress
from os import environ
from genefab3.exceptions import traceback_printer, exception_catcher
from genefab3.flask.parser import parse_request
from genefab3.mongo.meta import refresh_database_metadata
from genefab3.flask.display import display


app = Flask("genefab3")
COMPRESS_MIMETYPES = COMPRESSIBLE_MIMETYPES
Compress(app)


mongo = MongoClient(serverSelectionTimeoutMS=2000)
try:
    mongo.server_info()
except ServerSelectionTimeoutError:
    raise GeneLabDatabaseException("Could not connect (sensitive info hidden)")
else:
    db = getattr(mongo, MONGO_DB_NAME)


if environ.get("FLASK_ENV", None) in DEBUG_MARKERS:
    traceback_printer = app.errorhandler(Exception)(traceback_printer)
else:
    exception_catcher = app.errorhandler(Exception)(exception_catcher)


def get_and_display(db, getter, kwargs, request):
    """Wrapper for data retrieval and display"""
    context = parse_request(request)
    refresh_database_metadata(db, context.select)
    return display(getter(db, **kwargs, context=context), context)


@app.route("/", methods=["GET"])
def documentation():
    from genefab3.docs import interactive_doc
    return interactive_doc(url_root=request.url_root.rstrip("/"))

@app.route("/assays/", methods=["GET"])
def assays(**kwargs):
    from genefab3.flask.meta import get_assays_by_metas as getter
    return get_and_display(db, getter, kwargs, request)

@app.route("/samples/", methods=["GET"])
def samples(**kwargs):
    from genefab3.flask.meta import get_samples_by_metas as getter
    return get_and_display(db, getter, kwargs, request)

@app.route("/data/", methods=["GET"])
def data(**kwargs):
    from genefab3.flask.data import get_data_by_metas as getter
    return get_and_display(db, getter, kwargs, request)

@app.route("/favicon.<imgtype>")
def favicon(**kwargs):
    return ""


# Debug zone:

@app.route("/debug/")
def debug():
    """Debug"""
    from genefab3.flask.debug import debug
    return debug(db)

@app.route("/debug/<meta>/", methods=["GET"])
def meta(**kwargs):
    """List names of particular meta"""
    context = parse_request(request)
    refresh_database_metadata(db)
    from genefab3.flask.meta import get_meta_names as getter
    return display(getter(db, **kwargs, context=context), context)

@app.route("/debug/<accession>/<assay_name>/<meta>/", methods=["GET"])
def assay_metadata(**kwargs):
    """Display assay metadata"""
    context = parse_request(request)
    from genefab3.flask.debug import get_assay_metadata as getter
    return display(getter(db, **kwargs, rargs=request.args), context)
