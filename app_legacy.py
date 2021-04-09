#!/usr/bin/env python
from flask import request
from genefab3.frontend.renderer import render
from collections import namedtuple

def data(**kwargs):
    from genefab3.frontend.getters.data import get_data as getter
    dbs = namedtuple("dbs", "mongo_db, sqlite_db")("mongo_db", "SQLITE_DB")
    return render(dbs, getter, kwargs, request)
