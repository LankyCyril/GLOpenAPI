#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.exceptions import GeneLabDatabaseException
from genefab3.config import MONGO_DB_NAME, DEBUG_MARKERS, COMPRESSIBLE_MIMETYPES
from flask import Flask, request
from flask_compress import Compress
from os import environ
from genefab3.exceptions import traceback_printer, exception_catcher
from genefab3.mongo.meta import parse_assay_selection, refresh_database_metadata
from genefab3.display import display


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


@app.route("/", methods=["GET"])
def documentation():
    """Hello, Space!"""
    from genefab3.docs import interactive_doc
    return interactive_doc(url_root=request.url_root.rstrip("/"))

@app.route("/<meta>/", methods=["GET"])
def meta(**kwrags):
    """List names of particular meta"""
    refresh_database_metadata(db)
    from genefab3.flask.meta import get_meta_names as getter
    return display(getter(db, **kwrags, rargs=request.args), request)

@app.route("/assays/", methods=["GET"])
@app.route("/assays/<meta>/", methods=["GET"])
def assays(**kwargs):
    """Select assays based on annotation filters"""
    assay_selection = parse_assay_selection(request.args.getlist("select"))
    refresh_database_metadata(db, assay_selection)
    from genefab3.flask.assays import get_assays_by_metas as getter
    return display(getter(db, **kwargs, rargs=request.args), request)

@app.route("/samples/", methods=["GET"])
def samples(**kwargs):
    """Select samples based on annotation filters"""
    refresh_database_metadata(db)
    from genefab3.flask.data import get_samples_by_metas as getter
    return display(getter(db, **kwargs, rargs=request.args), request)

@app.route("/data/", methods=["GET"])
def data(**kwargs):
    """Select data based on annotation filters"""
    refresh_database_metadata(db)
    from genefab3.flask.data import get_data_by_metas as getter
    return display(getter(db, **kwargs, rargs=request.args), request)

@app.route("/<accession>/<assay_name>/<meta>/", methods=["GET"])
def assay_metadata(**kwargs):
    """Display assay metadata"""
    from genefab3.flask.debug import get_assay_metadata as getter
    return display(getter(db, **kwargs, rargs=request.args), request)

@app.route("/favicon.<imgtype>")
def favicon(**kwargs):
    """Catch request for favicons"""
    return ""

@app.route("/debug/")
def debug():
    """Debug"""
    from genefab3.flask.debug import debug
    return debug(db)
