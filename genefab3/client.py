from pymongo import MongoClient
from numpy import base_repr
from datetime import datetime
from socket import create_connection, error as SocketError
from genefab3.common.exceptions import GeneFabConfigurationException
from sqlite3 import connect, OperationalError
from types import SimpleNamespace
from flask_compress import Compress
from genefab3.api.renderer import CacheableRenderer
from genefab3.common.logger import GeneFabLogger, MongoDBLogger
from functools import partial
from genefab3.common.exceptions import exception_catcher
from genefab3.common.utils import is_debug
from genefab3.db.cacher import CacherThread


class GeneFabClient():
    """Routes Response-generating methods, continuously caches metadata and responses"""
 
    def __init__(self, *, AdapterClass, RoutesClass, mongo_params, sqlite_params, cacher_params, flask_params):
        """Initialize metadata cacher (with adapter), response cacher, routes"""
        try:
            self.flask_app = self._configure_flask_app(**flask_params)
            self.mongo_collections, self.locale, self.units_formatter = (
                self._get_mongo_db_connection(**mongo_params)
            )
            self.sqlite_dbs = self._get_validated_sqlite_dbs(**sqlite_params)
        except TypeError as e:
            msg = "During GeneFabClient() initialization, exception occurred"
            raise GeneFabConfigurationException(msg, error=repr(e))
        else:
            self.adapter = AdapterClass()
            self.routes = RoutesClass(
                self.mongo_collections, locale=self.locale,
                sqlite_dbs=self.sqlite_dbs, adapter=self.adapter,
            )
            self.cacher_params = cacher_params
            self._init_routes()
            self._init_warning_handlers()
            self._init_error_handlers()
 
    def _configure_flask_app(self, *, app, compress_params=None):
        """Modify Flask application, enable compression"""
        app.config = {**getattr(app, "config", {}), **(compress_params or {})}
        Compress(app)
        return app
 
    def _get_mongo_db_connection(self, *, db_name, client_params=None, collection_names=None, locale="en_US", units_formatter=None, test_timeout=10):
        """Check MongoDB server is running, connect to database `db_name`"""
        _ts_36 = base_repr(int(datetime.now().timestamp() * 1000000), 36)
        self._mongo_appname = f"genefab3 {_ts_36}"
        _kw = dict(**(client_params or {}), appname=self._mongo_appname)
        self._mongo_client = MongoClient(**_kw)
        try:
            host_and_port = (self._mongo_client.HOST, self._mongo_client.PORT)
            with create_connection(host_and_port, timeout=test_timeout):
                pass
        except SocketError as e:
            msg = "Could not connect to internal MongoDB instance"
            raise GeneFabConfigurationException(msg, error=type(e).__name__)
        parsed_cnames = {
            kind: (collection_names or {}).get(kind, kind)
            for kind in ("metadata", "metadata_aux", "records", "status", "log")
        }
        for kind in ("metadata", "metadata_aux", "records", "status"):
            if parsed_cnames[kind] is None:
                msg = "Collection name cannot be None"
                raise GeneFabConfigurationException(msg, collection=kind)
        if len(parsed_cnames) != len(set(parsed_cnames.values())):
            msg = "Conflicting collection names specified"
            raise GeneFabConfigurationException(msg, names=parsed_cnames)
        else:
            mongo_collections = SimpleNamespace(**{
                kind: (self._mongo_client[db_name][cname] if cname else None)
                for kind, cname in parsed_cnames.items()
            })
        return mongo_collections, locale, units_formatter
 
    def _get_validated_sqlite_dbs(self, *, blobs, tables, cache=None):
        """Check target SQLite3 files are specified correctly, convert to namespace for dot-syntax lookup"""
        if len({blobs, tables, cache}) != 3:
            msg = "SQL databases must all be distinct to avoid name conflicts"
            _kw = dict(blobs=blobs, tables=tables, cache=cache)
            raise GeneFabConfigurationException(msg, **_kw)
        elif (not isinstance(blobs, str)) or (not isinstance(tables, str)):
            msg = "SQL databases must be file paths"
            raise GeneFabConfigurationException(msg, blobs=blobs, tables=tables)
        elif (not isinstance(cache, str)) and (cache is not None):
            msg = "SQL database must be a file path or None"
            raise GeneFabConfigurationException(msg, cache=cache)
        else:
            sqlite_dbs = dict(blobs=blobs, tables=tables, cache=cache)
            for name, filename in ((n, f) for n, f in sqlite_dbs.items() if f):
                try:
                    connect(filename).close()
                except OperationalError:
                    msg = "SQL database not reachable"
                    raise GeneFabConfigurationException(msg, name=filename)
            return SimpleNamespace(**sqlite_dbs)
 
    def _init_routes(self):
        """Route Response-generating methods to Flask endpoints"""
        renderer = CacheableRenderer(self.sqlite_dbs, self.flask_app)
        for endpoint, method in self.routes.items():
            self.flask_app.route(endpoint, methods=["GET"])(renderer(method))
 
    def _init_warning_handlers(self):
        """Set up logger to write to MongoDB collection if specified"""
        GeneFabLogger().addHandler(MongoDBLogger(self.mongo_collections.log))
 
    def _init_error_handlers(self):
        """Intercept all exceptions and deliver an HTTP error page with or without traceback depending on debug state"""
        _k = dict(collection=self.mongo_collections.log, debug=is_debug())
        self.flask_app.errorhandler(Exception)(partial(exception_catcher, **_k))
 
    def _ok_to_loop(self):
        """Check if no other instances of genefab3 are talking to MongoDB database"""
        if self.cacher_params.get("enabled") is False:
            msg = "CacherThread disabled by client parameter, NOT LOOPING"
            GeneFabLogger().info(f"{self._mongo_appname}: {msg}")
            return False
        else:
            query = {"$currentOp": {"allUsers": True, "idleConnections": True}}
            projection = {"$project": {"appName": True}}
            for e in self._mongo_client.admin.aggregate([query, projection]):
                other = e.get("appName", "")
                if other.startswith("genefab3"):
                    if other != self._mongo_appname:
                        msg = (f"Found other instance ({other}), " +
                            "NOT LOOPING current instance")
                        GeneFabLogger().info(f"{self._mongo_appname}: {msg}")
                        return False
            else:
                msg = "No other instances found, STARTING LOOP"
                GeneFabLogger().info(f"{self._mongo_appname}: {msg}")
                return True
 
    def loop(self):
        """Start background cacher thread"""
        if self._ok_to_loop():
            try:
                cacher_thread_params = dict(
                    adapter=self.adapter,
                    mongo_collections=self.mongo_collections,
                    mongo_appname=self._mongo_appname, locale=self.locale,
                    units_formatter=self.units_formatter,
                    sqlite_dbs=self.sqlite_dbs, **self.cacher_params,
                )
                CacherThread(**cacher_thread_params).start()
            except TypeError as e:
                msg = "Incorrect cacher_params for GeneFabClient()"
                raise GeneFabConfigurationException(msg, error=repr(e))
