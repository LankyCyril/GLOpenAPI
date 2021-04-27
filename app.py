#!/usr/bin/env python
from flask import Flask
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.api.routes import DefaultRoutes

flask_app = Flask("NASA GeneLab Data API")

genefab3_client = GeneFabClient(
    AdapterClass=GeneLabAdapter,
    RoutesClass=DefaultRoutes,
    mongo_params=dict(
        db_name="genefab3",
        locale="en_US",
        client_params={},
        units_formatter="{value} {{{unit}}}".format,
    ),
    sqlite_params=dict(
        blobs="./.genefab3.sqlite3/blobs.db",
        tables="./.genefab3.sqlite3/tables.db",
        response_cache="./.genefab3.sqlite3/response-cache.db",
        response_cache_size=24*1024*1024*1024,
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
)

genefab3_client.loop()
