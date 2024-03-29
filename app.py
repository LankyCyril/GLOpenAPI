#!/usr/bin/env python
from flask import Flask
from genefab3.client import GeneFabClient
from genefab3_genelab_adapter import GeneLabAdapter
from genefab3.api.routes import DefaultRoutes


# Initialize the Flask app.
# - Must be defined in the global scope, i.e. right here, even though all we are
#   doing later is passing `flask_app` to `GeneFabClient` via `flask_params`.
#   Defining it in the global scope is meant to accommodate the standard ways of
#   setting up the server and testing the app, because loaders usually rely on
#   importing the app variable from this file.
# - The name of the Flask app will be used in the header of the landing page.

flask_app = Flask("NASA GeneLab Open API")
__version__ = "3.0.2"
GiB = 1024**3


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
    app_version=__version__,
    AdapterClass=GeneLabAdapter,
    RoutesClass=DefaultRoutes,
    mongo_params=dict(
        db_name="genefab3", locale="en_US",
        units_formatter="{value} {{{unit}}}".format, # `f(value, unit) -> str`
        client_params={}, # any other `pymongo.MongoClient()` parameters
    ),
    sqlite_params=dict( # the SQLite databases are LRU if capped by `maxsize`:
        blobs=dict(
            db="./.genefab3.sqlite3/blobs.db", maxsize=None, # required;
                # stores up-to-date ISA data
        ),
        tables=dict(
            db="./.genefab3.sqlite3/tables.db", maxsize=48*GiB, # required;
                # stores cacheable tabular data
        ),
        response_cache=dict(
            db="./.genefab3.sqlite3/response-cache.db", maxsize=24*GiB,
                # optional, pass `db=None` to disable; caches displayable
                # results of user requests until the (meta)data changes
        ),
    ),
    metadata_cacher_params=dict(
        enabled=True,
        dataset_init_interval=3, # seconds between adding datasets that have not
            # been previously cached
        dataset_update_interval=60, # seconds between updating datasets that
            # have (potentially stale) cache
        full_update_interval=0, # seconds between full update cycles;
            # each update cycle already takes at least
            # `dataset_update_interval * n_datasets_in_local_database` seconds,
            # and on average the app will only ping the backing cold storage
            # approximately once every `dataset_update_interval` seconds;
            # therefore, the value of `full_update_interval` can be as low as 0,
            # but if you want the cacher to additionally sleep between these
            # cycles, set this value to whatever you like
        full_update_retry_delay=600, # seconds before retrying the full update
            # cycle if the cold storage server was unreachable
    ),
    flask_params=dict(
        app=flask_app,
        compress_params=dict(COMPRESS_MIMETYPES=[ # passed to `flask_compress`
            "text/plain", "text/html", "text/css", "text/xml",
            "application/json", "application/javascript",
        ]),
    ),
)
