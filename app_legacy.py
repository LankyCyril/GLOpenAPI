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

@app.route("/data/", methods=["GET"])
def data(**kwargs):
    from genefab3.frontend.getters.data import get_data as getter
    dbs = namedtuple("dbs", "mongo_db, sqlite_db")(mongo_db, SQLITE_DB)
    return render(dbs, getter, kwargs, request)
