#!/usr/bin/env python
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.exceptions import GeneLabDatabaseException
from genefab3.config import MONGO_DB_NAME, COMPRESSIBLE_MIMETYPES, SQLITE_DB
from genefab3.utils import is_debug
from flask import Flask, request
from flask_compress import Compress
from os import environ
from genefab3.exceptions import traceback_printer, exception_catcher, DBLogger
from logging import getLogger
from functools import partial
from genefab3.mongo.cacher import CacherThread
from genefab3.flask.display import display
from argparse import Namespace


# Backend initialization:

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

if environ.get("WERKZEUG_RUN_MAIN", None) != "true":
    # https://stackoverflow.com/a/9476701/590676
    CacherThread(db).start()

if is_debug():
    traceback_printer = app.errorhandler(Exception)(
        partial(traceback_printer, db=db),
    )
else:
    exception_catcher = app.errorhandler(Exception)(
        partial(exception_catcher, db=db),
    )
getLogger("genefab3").addHandler(DBLogger(db))


# App routes:

@app.route("/", methods=["GET"])
def documentation():
    from genefab3.docs import interactive_doc
    return interactive_doc(db, url_root=request.url_root.rstrip("/"))

@app.route("/assays/", methods=["GET"])
def assays(**kwargs):
    from genefab3.flask.meta import get_assays_by_metas as getter
    return display(db, getter, kwargs, request)

@app.route("/samples/", methods=["GET"])
def samples(**kwargs):
    from genefab3.flask.meta import get_samples_by_metas as getter
    return display(db, getter, kwargs, request)

@app.route("/files/", methods=["GET"])
def files(**kwargs):
    from genefab3.flask.meta import get_files_by_metas as getter
    return display(db, getter, kwargs, request)

@app.route("/file/", methods=["GET"])
def file(**kwargs):
    from genefab3.flask.file import get_file as getter
    return display(db, getter, kwargs, request)

@app.route("/data/", methods=["GET"])
def data(**kwargs):
    from genefab3.flask.data import get_data_by_metas as getter
    dbs = Namespace(mongo_db=db, sqlite_db=SQLITE_DB)
    return display(dbs, getter, kwargs, request)

@app.route("/favicon.<imgtype>")
def favicon(**kwargs):
    return ""
