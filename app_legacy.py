#!/usr/bin/env python
from flask import request
from genefab3.config import SQLITE_DB
from genefab3.frontend.renderer import render
from collections import namedtuple

mongo_db, app = None, None # lol

@app.route("/", methods=["GET"])
def root(**kwargs):
    from genefab3.frontend.renderers.docs import interactive_doc
    return interactive_doc(mongo_db, url_root=request.url_root.rstrip("/"))

@app.route("/assays/", methods=["GET"])
def assays(**kwargs):
    from genefab3.frontend.getters.metadata import get_assays as getter
    return render(mongo_db, getter, kwargs, request)

@app.route("/samples/", methods=["GET"])
def samples(**kwargs):
    from genefab3.frontend.getters.metadata import get_samples as getter
    return render(mongo_db, getter, kwargs, request)

@app.route("/files/", methods=["GET"])
def files(**kwargs):
    from genefab3.frontend.getters.metadata import get_files as getter
    return render(mongo_db, getter, kwargs, request)

@app.route("/file/", methods=["GET"])
def file(**kwargs):
    from genefab3.frontend.getters.file import get_file as getter
    return render(mongo_db, getter, kwargs, request)

@app.route("/data/", methods=["GET"])
def data(**kwargs):
    from genefab3.frontend.getters.data import get_data as getter
    dbs = namedtuple("dbs", "mongo_db, sqlite_db")(mongo_db, SQLITE_DB)
    return render(dbs, getter, kwargs, request)

@app.route("/status/", methods=["GET"])
def status(**kwargs):
    from genefab3.frontend.getters.status import get_status as getter
    return render(mongo_db, getter, kwargs, request)
