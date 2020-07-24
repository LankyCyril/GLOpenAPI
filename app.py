#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.exceptions import GeneLabDatabaseException
from genefab3.config import MONGO_DB_NAME, DEBUG_MARKERS, COMPRESSIBLE_MIMETYPES
from flask import Flask, request
from flask_compress import Compress
from os import environ
from genefab3.exceptions import traceback_printer, exception_catcher
from genefab3.mongo import refresh_json_store
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


@app.route("/favicon.<imgtype>")
def favicon(imgtype):
    """Catch request for favicons"""
    return ""


@app.route("/", methods=["GET"])
@refresh_json_store(db)
def landing_page():
    """Hello, Space!"""
    return interactive_doc(url_root=request.url_root.rstrip("/"))


@app.route("/debug", methods=["GET"])
def debug_page():
    from genefab3.mongo import refresh_json_store_inner
    all_accessions, fresh, stale = refresh_json_store_inner(db)
    return "<hr>".join([
        "All accessions:<br>" + ", ".join(sorted(all_accessions)),
        "Fresh accessions:<br>" + ", ".join(sorted(fresh)),
        "Stale accessions:<br>" + ", ".join(sorted(stale)),
    ])
