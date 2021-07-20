#!/usr/bin/env python
from flask import Flask
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.api.routes import DefaultRoutes

GiB = 1024**3

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
        blobs=dict(
            db="./.genefab3.sqlite3/blobs.db", maxsize=None,
        ),
        tables=dict(
            db="./.genefab3.sqlite3/tables.db", maxsize=48*GiB,
        ),
        response_cache=dict(
            db="./.genefab3.sqlite3/response-cache.db", maxsize=24*GiB,
        ),
    ),
    metadata_cacher_params=dict(
        enabled=True,
        full_update_interval=21600, # sec between full update cycles
        full_update_retry_delay=600, # sec before retrying if server unreachable
        dataset_update_interval=60, # sec between updating each dataset
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
