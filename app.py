#!/usr/bin/env python
from flask import Flask
from flask_compress import Compress
from genefab3.api.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.api.utils import is_flask_reloaded

flask_app = Flask("genefab3")
COMPRESS_MIMETYPES = [
    "text/plain", "text/html", "text/css", "text/xml",
    "application/json", "application/javascript",
]
Compress(flask_app)

genefab3_client = GeneFabClient(
    adapter=GeneLabAdapter,
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
        start_condition=lambda: not is_flask_reloaded(),
        interval=1800,
        recheck_delay=300,
    ),
    flask_params=dict(
        app=flask_app,
    ),
    logger_params=dict(
        mongo_collection="log",
        stderr=True,
    ),
)

genefab3_client.loop()
