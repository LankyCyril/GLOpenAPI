#!/usr/bin/env python
from flask import Flask
from flask_compress import Compress
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from os import environ

flask_app = Flask("genefab3")
COMPRESS_MIMETYPES = [
    "text/plain", "text/html", "text/css", "text/xml",
    "application/json", "application/javascript",
]
Compress(flask_app)

genefab3_client = GeneFabClient(
    adapter=GeneLabAdapter,
    flask_app=flask_app,
    mongo_params=dict(
        db_name="genefab3_testing",
        locale="en_US",
        client_params={},
    ),
    sqlite_params=dict(
        blobs="./.sqlite3/blobs.db",
        tables="./.sqlite3/tables.db",
        cache="./.sqlite3/response-cache.db",
    ),
    cacher_params=dict(
        start_condition=lambda: environ.get("WERKZEUG_RUN_MAIN") != "true",
        interval=1800,
        recheck_delay=300,
    ),
)

genefab3_client.loop()
