#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.exceptions import GeneLabException, GeneLabDatabaseException
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


@app.route("/assays/", methods=["GET"])
def assays():
    """Select assays based on annotation filters"""
    unknown_args = set(request.args) - {
        "factors", "comments", "characteristics", "properties",
    }
    if unknown_args:
        raise GeneLabException("Unrecognized arguments: {}".format(
            ", ".join(sorted(unknown_args))
        ))
    try:
        datasets_and_assays = set.intersection(*(
            {
                (entry["accession"], entry["assay_name"]) for entry in
                db.assay_properties.find({
                    "property": "factors",
                    "field": {"$in": factors_any.split("|")},
                })
            }
            for factors_any in request.args.getlist("factors")
        ))
    except TypeError:
        datasets_and_assays = {}
    return "<pre>Dataset  \tAssay\n" + "\n".join([
        "{} \t{}".format(accession, assay_name)
        for accession, assay_name in sorted(datasets_and_assays)
    ])


@app.route("/debug", methods=["GET"])
def debug_page():
    from genefab3.mongo import refresh_json_store_inner
    all_accessions, fresh, stale = refresh_json_store_inner(db)
    return "<hr>".join([
        "All accessions:<br>" + ", ".join(sorted(all_accessions)),
        "Fresh accessions:<br>" + ", ".join(sorted(fresh)),
        "Stale accessions:<br>" + ", ".join(sorted(stale)),
    ])
