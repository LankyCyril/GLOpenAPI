#!/usr/bin/env python
from os import environ
from flask import Flask
from glopenapi.client import GLOpenAPIClient
from glopenapi_genelab_adapter import GeneLabAdapter
from glopenapi.api.routes import DefaultRoutes


# Initialize the Flask app.
# - Must be defined in the global scope, i.e. right here, even though all we are
#   doing later is passing `flask_app` to `GLOpenAPIClient` via `flask_params`.
#   Defining it in the global scope is meant to accommodate the standard ways of
#   setting up the server and testing the app, because loaders usually rely on
#   importing the app variable from this file.
# - The name of the Flask app will be used in the header of the landing page.

flask_app = Flask("NASA GeneLab Open API")
__version__ = "4.0.8-alpha3"


# If MODE is 'nocache' (e.g., `export MODE=nocache` in wrapper, or running in
# debug mode as FLASK_ENV=development MODE=nocache FLASK_APP=app.py flask run),
# will disable the continuous MetadataCacherLoop as well as the response_cache
# SQLite3 database file (see below):

NOCACHE = (environ.get("MODE") == "nocache")
GiB = 1024**3


# By default, GLOpenAPI will try to read data from osdr.nasa.gov; this can
# be changed by setting the environment variable GENELAB_ROOT, e.g.
# GENELAB_ROOT=my.awesome.site FLASK_APP=app.py flask run:

if "GENELAB_ROOT" in environ:
    GENELAB_ROOT = [environ["GENELAB_ROOT"]]
else:
    GENELAB_ROOT = "https://osdr.nasa.gov", "https://genelab-data.ndc.nasa.gov"


# Initialize the glopenapi client.
# - `adapter` must be of subclass of `glopenapi.common.types.Adapter()`
#      and provide the following methods:
#      - `get_accessions()`: returns an iterable of accession names;
#      - `get_files_by_accession(accession)`: returns a dictionary of the form
#             {"%FILENAME%": {
#                 "timestamp" 123456789, # UNIX epoch rounded to an integer
#                 "urls": ["%URL1%", "%URL2%", ...], # first reachable is used
#             }}
#         Each entry can have additional optional fields; for more details,
#         see documentation in glopenapi/common/types.py,
#         or an implementation in glopenapi_genelab_adapter/adapter.py.
# - `RoutesClass` must be a subclass of `glopenapi.api.types.Routes()`
#      and associate endpoints with functions that may return objects of various
#      types understood by `glopenapi.api.renderer.CacheableRenderer()`.
#      See implementation of `DefaultRoutes` in glopenapi/api/routes.py,
#      and `TYPE_RENDERERS` in glopenapi/api/renderer.py.
# - `mongo_params`, `sqlite_params`, `metadata_cacher_params`, `flask_params`
#      are passed as keyword arguments to methods of `GLOpenAPIClient`;
#      for possible argument names, see glopenapi/client.py.
#      - `mongo_params` is passed to `_get_mongo_db_connection()`;
#      - `sqlite_params` to `_get_validated_sqlite_dbs()`;
#      - `metadata_cacher_params` to `_ensure_cacher_loop_thread()`
#         - and then to `glopenapi.db.mongo.cacher.MetadataCacherLoop()`;
#      - `flask_params` to `_configure_flask_app()`

glopenapi_client = GLOpenAPIClient(
    app_version=__version__,
    adapter=GeneLabAdapter(root_urls=GENELAB_ROOT),
    RoutesClass=DefaultRoutes,
    mongo_params=dict(
        db_name="genefab3", locale="en_US",
        units_formatter="{value} {{{unit}}}".format, # `f(value, unit) -> str`
        client_params={}, # any other `pymongo.MongoClient()` parameters
    ),
    sqlite_params=dict( # the SQLite3 databases are LRU if capped by `maxsize`:
        blobs=dict( # stores up-to-date ISA data; required:
            db="./.genefab3.sqlite3/blobs.db", maxsize=None,
        ),
        tables=dict( # stores cacheable tabular data; required:
            db="./.genefab3.sqlite3/tables.db", maxsize=48*GiB,
        ),
        response_cache=dict( # optional! pass `db=None` to disable;
            # caches results of user requests until the (meta)data changes:
            db=(None if NOCACHE else "./.genefab3.sqlite3/response-cache.db"),
            maxsize=24*GiB, min_app_version="4.0.8-alpha2",
        ),
    ),
    metadata_cacher_params=dict(
        enabled=(not NOCACHE),
        dataset_init_interval=3, # seconds between adding datasets that have not
            # been previously cached
        dataset_update_interval=60, # seconds between updating datasets that
            # have (potentially stale) cache
        full_update_interval=0, # seconds between full update cycles;
            # each update cycle already takes at least
            # `dataset_update_interval * n_datasets_in_local_database` seconds,
            # and so on average the app will only ping the backing cold storage
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
