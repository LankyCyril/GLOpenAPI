from glopenapi.db.sql.response_cache import ResponseCache
from functools import partial
from glopenapi.isa.types import Dataset
from glopenapi.common.exceptions import GLOpenAPIConfigurationException
from glopenapi.common.exceptions import is_debug
from flask_compress import Compress
from glopenapi.common.utils import timestamp36
from pymongo import MongoClient
from socket import create_connection, error as SocketError
from types import SimpleNamespace
from glopenapi.common.exceptions import GLOpenAPILogger, exception_catcher
from glopenapi.db.mongo.utils import iterate_mongo_connections
from glopenapi.db.mongo.cacher import MetadataCacherLoop
from glopenapi.api.renderer import CacheableRenderer
from glopenapi.api.parser import Context
from threading import Thread


class GLOpenAPIClient():
    """Routes Response-generating methods, continuously caches metadata and responses"""
 
    def __init__(self, *, adapter, RoutesClass, mongo_params, sqlite_params, metadata_cacher_params, flask_params, app_version="0"):
        """Initialize metadata cacher (with adapter), response cacher, routes"""
        self.app_version = app_version
        try:
            self.flask_app = self._configure_flask_app(**flask_params)
            (self.mongo_client, self.mongo_collections, self.locale,
             self.units_formatter) = (
                self._get_mongo_db_connection(**mongo_params)
            )
            self.sqlite_dbs = self._get_validated_sqlite_dbs(**sqlite_params)
            self.adapter = adapter
            self._init_error_handlers()
            self.renderer, self.routes = self._init_routes(RoutesClass)
            self.response_cache = ResponseCache(self.sqlite_dbs)
            self.DatasetConstructor = partial(
                Dataset, sqlite_dbs=self.sqlite_dbs,
            )
            self.cacher_loop_thread = self._ensure_cacher_loop_thread(
                **metadata_cacher_params,
            )
        except TypeError as exc:
            msg = "Exception occurred during GLOpenAPIClient() initialization"
            raise GLOpenAPIConfigurationException(msg, debug_info=repr(exc))
 
    def _configure_flask_app(self, *, app, compress_params=None):
        """Modify Flask application, enable compression"""
        app.config = {**getattr(app, "config", {}), **(compress_params or {})}
        Compress(app)
        @app.after_request
        def apply_headers(response):
            response.headers["X-Frame-Options"] = "SAMEORIGIN"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Content-Security-Policy"] = "; ".join([
                "default-src 'self' fonts.gstatic.com",
                "style-src-attr 'self' 'unsafe-inline'",
                "frame-ancestors 'self'",
                "form-action 'self'",
                "style-src 'self' fonts.googleapis.com",
                "connect-src 'self'",
                "img-src 'self' data: genelab.nasa.gov genelab-data.ndc.nasa.gov",
                "script-src 'self'",
            ])
            return response
        return app
 
    def _get_mongo_db_connection(self, *, db_name, client_params=None, collection_names=None, locale="en_US", units_formatter=None, test_timeout=10):
        """Check MongoDB server is running, connect to database `db_name`"""
        self.mongo_appname = f"GLOpenAPI({timestamp36()})"
        mongo_client = MongoClient(
            maxIdleTimeMS=60000, **(client_params or {}),
            appname=self.mongo_appname,
        )
        try:
            host_and_port = (mongo_client.HOST, mongo_client.PORT)
            with create_connection(host_and_port, timeout=test_timeout):
                pass
        except SocketError as exc:
            msg = "Could not connect to internal MongoDB instance"
            raise GLOpenAPIConfigurationException(msg, error=type(exc).__name__)
        parsed_cnames = {
            kind: (collection_names or {}).get(kind) or kind
            for kind in ("metadata", "metadata_aux", "records", "status")
        }
        if len(parsed_cnames) != len(set(parsed_cnames.values())):
            msg = "Conflicting collection names specified"
            raise GLOpenAPIConfigurationException(msg, debug_info=parsed_cnames)
        else:
            mongo_collections = SimpleNamespace(**{
                kind: (mongo_client[db_name][cname] if cname else None)
                for kind, cname in parsed_cnames.items()
            })
        return mongo_client, mongo_collections, locale, units_formatter
 
    def _get_validated_sqlite_dbs(self, *, blobs, tables, response_cache):
        """Check target SQLite3 files are specified correctly, convert to namespace for dot-syntax lookup"""
        sqlite_dbs = SimpleNamespace(
            blobs={**blobs, "app_version": self.app_version},
            tables={**tables, "app_version": self.app_version},
            response_cache={**response_cache, "app_version": self.app_version},
        )
        if len({v.get("db") for v in sqlite_dbs.__dict__.values()}) != 3:
            msg = "SQL databases must all be distinct to avoid name conflicts"
            _kw = dict(debug_info=sqlite_dbs.__dict__)
            raise GLOpenAPIConfigurationException(msg, **_kw)
        else:
            return sqlite_dbs
 
    def _init_error_handlers(self):
        """Intercept all exceptions and deliver an HTTP error page with or without traceback depending on debug state"""
        self.flask_app.errorhandler(Exception)(partial(
            exception_catcher, debug=is_debug(),
        ))
 
    def _init_routes(self, RoutesClass):
        """Route Response-generating methods to Flask endpoints"""
        def _cleanup_after_request():
            if not self.cacher_loop_thread.isAlive():
                self.mongo_client.close()
        routes = RoutesClass(glopenapi_client=self)
        renderer = CacheableRenderer(
            sqlite_dbs=self.sqlite_dbs,
            get_context=lambda: Context(self.flask_app),
            cleanup=_cleanup_after_request,
        )
        for endpoint, method in routes.items():
            self.flask_app.route(endpoint, methods=["GET"])(renderer(method))
        return renderer, routes
 
    def _ok_to_loop_cacher_loop_thread(self, enabled):
        """Check if no other instances of GLOpenAPI are talking to MongoDB database"""
        if not enabled:
            m = "MetadataCacherLoop disabled by client parameter, NOT LOOPING"
            GLOpenAPILogger.info(f"{self.mongo_appname}:\n  {m}")
            return False
        else:
            for other in iterate_mongo_connections(self.mongo_client):
                if other < self.mongo_appname:
                    m = (f"Found other instance {other}, " +
                        "NOT LOOPING cachers in current instance")
                    GLOpenAPILogger.info(f"{self.mongo_appname}:\n  {m}")
                    return False
            else:
                m = "No other instances found, STARTING CACHER LOOP"
                GLOpenAPILogger.info(f"{self.mongo_appname}:\n  {m}")
                return True
 
    def _ensure_cacher_loop_thread(self, full_update_interval, full_update_retry_delay, dataset_init_interval, dataset_update_interval, min_app_version, enabled=True):
        """Start background cacher thread"""
        if self._ok_to_loop_cacher_loop_thread(enabled):
            # Note that cached responses get dropped *after* each cacher cycle;
            # technically, this means that cached responses at any given moment
            # may lag behind the real data by as much as the duration of one
            # update cycle. This is also true for specifically pre-cached
            # responses e.g. glopenapi.common.hacks.precache_metadata_counts().
            # The assumption throughout this API is that most of the time, no
            # changes happen between the update cycles (the underlying data repo
            # is mostly static), thus for the majority of the time, both MongoDB
            # and `response_cache` are accurate, except for the infrequent cases
            # when they are outdated by a time delta of a single update cycle,
            # while remaining *consistent* among themselves.
            def _cacher_loop():
                metadata_cacher_loop = MetadataCacherLoop(
                    glopenapi_client=self,
                    full_update_interval=full_update_interval,
                    full_update_retry_delay=full_update_retry_delay,
                    dataset_init_interval=dataset_init_interval,
                    dataset_update_interval=dataset_update_interval,
                    min_app_version=min_app_version,
                )
                for accessions in metadata_cacher_loop():
                    if accessions["updated"]:
                        self.response_cache.drop_all()
                    else:
                        _fod_accs = accessions["failed"] | accessions["dropped"]
                        for acc in _fod_accs:
                            self.response_cache.drop_by_accession(acc)
                        if _fod_accs:
                            self.response_cache.drop_by_context(identity="root.js")
                    self.response_cache.shrink()
            cacher_loop_thread = Thread(target=_cacher_loop)
            cacher_loop_thread.start()
            return cacher_loop_thread
        else:
            return SimpleNamespace(isAlive=lambda: False)
