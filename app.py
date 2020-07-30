#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.exceptions import GeneLabDatabaseException
from genefab3.config import MONGO_DB_NAME, DEBUG_MARKERS, COMPRESSIBLE_MIMETYPES
from flask import Flask, request
from flask_compress import Compress
from os import environ
from genefab3.exceptions import traceback_printer, exception_catcher
from genefab3.mongo.meta import refresh_database_metadata
from genefab3.docs import interactive_doc


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
    refresh_database_metadata(db)
    return interactive_doc(url_root=request.url_root.rstrip("/"))

@app.route("/assays/", methods=["GET"])
@app.route("/assays/<meta>/", methods=["GET"])
def assays(**kwargs):
    """Select assays based on annotation filters"""
    refresh_database_metadata(db)
    from genefab3.flask.assays import get_assays_by_metas
    return get_assays_by_metas(db, **kwargs, rargs=request.args)

@app.route("/favicon.<imgtype>")
def favicon(**kwargs):
    """Catch request for favicons"""
    return ""

@app.route("/debug/")
def debug():
    """Debug"""
    if environ.get("FLASK_ENV", None) not in DEBUG_MARKERS:
        return "Production server, debug disabled"
    else:
        from genefab3.flask.debug import debug
        return debug(db)
