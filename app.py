#!/usr/bin/env python
from flask import Flask
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter

flask_app = Flask("genefab3")

genefab3_client = GeneFabClient(
    AdapterClass=GeneLabAdapter,
    mongo_params=dict(
        db_name="genefab3_testing",
        locale="en_US",
        client_params={},
        units_formatter="{value} {{{unit}}}".format,
    ),
    sqlite_params=dict(
        blobs="./.genefab3.sqlite3/blobs.db",
        tables="./.genefab3.sqlite3/tables.db",
        cache="./.genefab3.sqlite3/response-cache.db",
    ),
    cacher_params=dict(
        enabled=False,
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
)

genefab3_client.loop()
