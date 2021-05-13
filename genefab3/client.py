from genefab3.common.exceptions import GeneFabConfigurationException
from flask_compress import Compress
from genefab3.common.utils import timestamp36, is_debug
from pymongo import MongoClient
from socket import create_connection, error as SocketError
from types import SimpleNamespace
from genefab3.common.exceptions import GeneFabLogger, exception_catcher
from genefab3.db.mongo.utils import iterate_mongo_connections
from genefab3.db.cacher import CacherThread
from genefab3.api.renderer import CacheableRenderer
from functools import partial


class GeneFabClient():
    """Routes Response-generating methods, continuously caches metadata and responses"""
 
    def __init__(self, *, AdapterClass, RoutesClass, mongo_params, sqlite_params, cacher_params, flask_params):
        """Initialize metadata cacher (with adapter), response cacher, routes"""
        try:
            self.flask_app = self._configure_flask_app(**flask_params)
            (self.mongo_client, self.mongo_collections, self.locale,
             self.units_formatter) = (
                self._get_mongo_db_connection(**mongo_params)
            )
            self.sqlite_dbs = self._get_validated_sqlite_dbs(**sqlite_params)
            self.adapter = AdapterClass()
            self._init_error_handlers()
            self.routes = self._init_routes(RoutesClass)
            self.cacher_thread = self._ensure_cacher_thread(**cacher_params)
        except TypeError as e:
            msg = "During GeneFabClient() initialization, an exception occurred"
            raise GeneFabConfigurationException(msg, error=repr(e))
 
    def _configure_flask_app(self, *, app, compress_params=None):
        """Modify Flask application, enable compression"""
        app.config = {**getattr(app, "config", {}), **(compress_params or {})}
        Compress(app)
        return app
 
    def _get_mongo_db_connection(self, *, db_name, client_params=None, collection_names=None, locale="en_US", units_formatter=None, test_timeout=10):
        """Check MongoDB server is running, connect to database `db_name`"""
        self.mongo_appname = f"GeneFab3({timestamp36()})"
        _kw = dict(**(client_params or {}), appname=self.mongo_appname)
        mongo_client = MongoClient(**_kw)
        try:
            host_and_port = (mongo_client.HOST, mongo_client.PORT)
            with create_connection(host_and_port, timeout=test_timeout):
                pass
        except SocketError as e:
            msg = "Could not connect to internal MongoDB instance"
            raise GeneFabConfigurationException(msg, error=type(e).__name__)
        parsed_cnames = {
            kind: (collection_names or {}).get(kind) or kind
            for kind in ("metadata", "metadata_aux", "records", "status")
        }
        if len(parsed_cnames) != len(set(parsed_cnames.values())):
            msg = "Conflicting collection names specified"
            raise GeneFabConfigurationException(msg, names=parsed_cnames)
        else:
            mongo_collections = SimpleNamespace(**{
                kind: (mongo_client[db_name][cname] if cname else None)
                for kind, cname in parsed_cnames.items()
            })
        return mongo_client, mongo_collections, locale, units_formatter
 
    def _get_validated_sqlite_dbs(self, *, blobs, tables, response_cache):
        """Check target SQLite3 files are specified correctly, convert to namespace for dot-syntax lookup"""
        sqlite_dbs = SimpleNamespace(
            blobs=blobs, tables=tables, response_cache=response_cache,
        )
        if len({v.get("db") for v in sqlite_dbs.__dict__.values()}) != 3:
            msg = "SQL databases must all be distinct to avoid name conflicts"
            raise GeneFabConfigurationException(msg, **sqlite_dbs.__dict__)
        else:
            return sqlite_dbs
 
    def _init_error_handlers(self):
        """Intercept all exceptions and deliver an HTTP error page with or without traceback depending on debug state"""
        self.flask_app.errorhandler(Exception)(partial(
            exception_catcher, debug=is_debug(),
        ))
 
    def _init_routes(self, RoutesClass):
        """Route Response-generating methods to Flask endpoints"""
        routes = RoutesClass(genefab3_client=self)
        renderer = CacheableRenderer(self)
        for endpoint, method in routes.items():
            self.flask_app.route(endpoint, methods=["GET"])(renderer(method))
        return routes
 
    def _ok_to_loop_cacher_thread(self, enabled):
        """Check if no other instances of genefab3 are talking to MongoDB database"""
        if not enabled:
            msg = "CacherThread disabled by client parameter, NOT LOOPING"
            GeneFabLogger(info=f"{self.mongo_appname}:\n  {msg}")
            return False
        else:
            for other in iterate_mongo_connections(self.mongo_client):
                if other < self.mongo_appname:
                    msg = (f"Found other instance {other}, " +
                        "NOT LOOPING current instance")
                    GeneFabLogger(info=f"{self.mongo_appname}:\n  {msg}")
                    return False
            else:
                msg = "No other instances found, STARTING LOOP"
                GeneFabLogger(info=f"{self.mongo_appname}:\n  {msg}")
                return True
 
    def _ensure_cacher_thread(self, metadata_update_interval=1800, metadata_retry_delay=300, enabled=True):
        """Start background cacher thread"""
        if self._ok_to_loop_cacher_thread(enabled):
            cacher_thread = CacherThread(
                genefab3_client=self,
                metadata_update_interval=metadata_update_interval,
                metadata_retry_delay=metadata_retry_delay,
            )
            cacher_thread.start()
            return cacher_thread
        else:
            return SimpleNamespace(isAlive=lambda: False)
