#!/usr/bin/env python
from flask import Flask
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.api.routes import DefaultRoutes

GiB = 1024**3


# Initialize the Flask app.
# - Must be defined in the global scope, i.e. right here, even though all we are
#   doing later is passing `flask_app` to `GeneFabClient` via `flask_params`.
#   Defining it in the global scope is meant to accommodate the standard ways of
#   setting up the server, e.g. through wsgi, because the loaders there rely on
#   importing the app variable from this file.
# - The name of the Flask app will be used in the header of the landing page.

flask_app = Flask("NASA GeneLab Data API")


# Initialize the genefab3 client.
# - `AdapterClass` must be a subclass of `genefab3.common.types.Adapter()`
#      and provide the following methods:
#      - `get_accessions()`: returns an iterable of accession names;
#      - `get_files_by_accession(accession)`: returns a dictionary of the form
#             {"%FILENAME%": {
#                 "timestamp" 123456789, # UNIX epoch rounded to an integer
#                 "urls": ["%URL1%", "%URL2%", ...], # first reachable is used
#             }}
#         Each entry can have additional optional fields; for more details,
#         see documentation in genefab3/common/types.py,
#         or an implementation in genefab3_genelab_adapter/adapter.py.
# - `RoutesClass` must be a subclass of `genefab3.common.types.Routes()`
#      and associate endpoints with functions that may return objects of various
#      types understood by `genefab3.api.renderer.CacheableRenderer()`.
#      See implementation of `DefaultRoutes` in genefab3/api/routes.py,
#      and `TYPE_RENDERERS` in genefab3/api/renderer.py.
# - `mongo_params`, `sqlite_params`, `metadata_cacher_params`, `flask_params`
#      are passed as keyword arguments to methods of `GeneFabClient`;
#      for possible argument names, see genefab3/client.py.
#      - `mongo_params` is passed to `_get_mongo_db_connection()`;
#      - `sqlite_params` to `_get_validated_sqlite_dbs()`;
#      - `metadata_cacher_params` to `_ensure_metadata_cacher_thread()`
#         - and then to `genefab3.db.cacher.MetadataCacherThread()`;
#      - `flask_params` to `_configure_flask_app()`

genefab3_client = GeneFabClient(
    AdapterClass=GeneLabAdapter,
    RoutesClass=DefaultRoutes,
    mongo_params=dict(
        db_name="genefab3", locale="en_US",
        units_formatter="{value} {{{unit}}}".format, # `f(value, unit) -> str`
        client_params={}, # any other `pymongo.MongoClient()` parameters
    ),
    sqlite_params=dict( # the SQLite databases are LRU if capped by `maxsize`:
        blobs=dict(
            # the blobs database stores up-to-date ISA data and is required:
            db="./.genefab3.sqlite3/blobs.db", maxsize=None,
        ),
        tables=dict(
            # the tables database stores cacheable tabular data and is required:
            db="./.genefab3.sqlite3/tables.db", maxsize=48*GiB,
        ),
        response_cache=dict(
            # the response_cache caches displayable results of user requests
            # until the (meta)data changes; optional, pass `db=None` to disable:
            db="./.genefab3.sqlite3/response-cache.db", maxsize=24*GiB,
        ),
    ),
    metadata_cacher_params=dict(
        enabled=True, # whether to spawn a persistent cacher daemon thread
        full_update_interval=21600, # sec between full update cycles
        full_update_retry_delay=600, # sec before retrying if server unreachable
        dataset_update_interval=60, # sec between updating each dataset
    ),
    flask_params=dict(
        app=flask_app,
        compress_params=dict(COMPRESS_MIMETYPES=[ # passed to `flask_compress`
            "text/plain", "text/html", "text/css", "text/xml",
            "application/json", "application/javascript",
        ]),
    ),
)
