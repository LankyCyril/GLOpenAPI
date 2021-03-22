#!/usr/bin/env python
from flask import Flask
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.api.utils import is_debug

flask_app = Flask("genefab3")

genefab3_client = GeneFabClient(
    AdapterClass=GeneLabAdapter,
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
        metadata_update_interval=1800,
        metadata_retry_delay=300,
    ),
    flask_params=dict(
        app=flask_app,
        compress_params=dict(
            COMPRESS_MIMETYPES=[
                "text/plain", "text/html", "text/css", "text/xml",
                "application/json", "application/javascript",
            ],
        ),
    ),
    logger_params=dict(
        mongo_collection_name="log",
        stderr=is_debug(),
    ),
)

genefab3_client.loop()
