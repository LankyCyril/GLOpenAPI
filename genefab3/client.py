from pymongo import MongoClient
from socket import create_connection, error as SocketError
from genefab3.common.exceptions import GeneFabConfigurationException
from sqlite3 import connect, OperationalError
from types import SimpleNamespace
from flask_compress import Compress
from genefab3.api.routes import Routes
from functools import partial
from genefab3.common.logger import GeneFabLogger, MongoDBLogger
from logging import DEBUG
from genefab3.api.utils import is_debug, is_flask_reloaded
from genefab3.db.mongo.cacher import CacherThread
from genefab3.common.exceptions import traceback_printer, exception_catcher


class GeneFabClient():
    """Routes Response-generating methods, continuously caches metadata and responses"""
 
    def __init__(self, *, AdapterClass, mongo_params, sqlite_params, cacher_params, flask_params, logger_params=None):
        """Initialize metadata cacher (with adapter), response cacher, routes"""
        try:
            self.flask_app = self._configure_flask_app(**flask_params)
            self.mongo_db, self.locale, self.units_formatter = (
                self._get_mongo_db_connection(**mongo_params)
            )
            self.sqlite_dbs = self._get_validated_sqlite_dbs(**sqlite_params)
            self._init_routes(Routes())
            self._init_warning_handlers(**logger_params)
            self._init_error_handlers(**logger_params)
        except TypeError as e:
            msg = f"During GeneFabClient() initialization, {e}"
            raise GeneFabConfigurationException(msg)
        else:
            self.adapter, self.cacher_params = AdapterClass(), cacher_params
 
    def _configure_flask_app(self, *, app, compress_params=None):
        """Modify Flask application, enable compression"""
        app.config = {**getattr(app, "config", {}), **(compress_params or {})}
        Compress(app)
        return app
 
    def _get_mongo_db_connection(self, *, db_name, client_params, locale="en_US", units_formatter=None, test_timeout=10):
        """Check MongoDB server is running, connect to database `db_name`"""
        mongo_client = MongoClient(**client_params)
        try:
            host_and_port = (mongo_client.HOST, mongo_client.PORT)
            with create_connection(host_and_port, timeout=test_timeout):
                pass
        except SocketError as e:
            msg = "Could not connect to internal MongoDB instance"
            raise GeneFabConfigurationException(msg, error=type(e).__name__)
        else:
            return mongo_client[db_name], locale, units_formatter
 
    def _get_validated_sqlite_dbs(self, *, blobs, tables, cache=None):
        """Check target SQLite3 files are specified correctly, convert to namespace for dot-syntax lookup"""
        if len({blobs, tables, cache}) != 3:
            msg = "SQL databases must all be distinct to avoid name conflicts"
            raise GeneFabConfigurationException(msg)
        elif (not isinstance(blobs, str)) or (not isinstance(tables, str)):
            msg = "SQL databases `blobs` and `tables` must be file paths"
            raise GeneFabConfigurationException(msg)
        elif (not isinstance(cache, str)) and (cache is not None):
            msg = "SQL database `cache` must be a file path or None"
            raise GeneFabConfigurationException(msg)
        else:
            sqlite_dbs = SimpleNamespace(
                blobs=blobs, tables=tables, cache=cache,
            )
            for name, filename in sqlite_dbs.__dict__.items():
                if filename:
                    try:
                        with connect(filename):
                            pass
                    except OperationalError:
                        msg = f"SQL database `{name}` not reachable"
                        raise GeneFabConfigurationException(msg)
            return sqlite_dbs
 
    def _init_routes(self, routes):
        """Route Response-generating methods to Flask endpoints"""
        for endpoint, method in routes.items():
            self.flask_app.route(endpoint, methods=["GET"])(method)
 
    def _init_warning_handlers(self, *, mongo_collection_name=None, print_handled_exceptions_to_stderr=False, level=DEBUG):
        """Set up logger to write to MongoDB collection and/or to stderr"""
        logger = GeneFabLogger()
        logger.setLevel(level)
        if mongo_collection_name is not None:
            collection = self.mongo_db[mongo_collection_name]
            logger.addHandler(MongoDBLogger(collection))
 
    def _init_error_handlers(self, *, mongo_collection_name=None, print_handled_exceptions_to_stderr=False, level=None):
        """Intercept all exceptions and deliver an HTTP error page with or without traceback depending on debug state"""
        if mongo_collection_name is not None:
            collection = self.mongo_db[mongo_collection_name]
        else:
            collection = None
        self.flask_app.errorhandler(Exception)(partial(
            traceback_printer if is_debug() else exception_catcher,
            collection=collection,
            print_to_stderr=print_handled_exceptions_to_stderr,
        ))
 
    def loop(self):
        """Start background cacher thread"""
        if not is_flask_reloaded():
            try:
                cacher_thread_params = dict(
                    adapter=self.adapter, mongo_db=self.mongo_db,
                    units_formatter=self.units_formatter,
                    sqlite_dbs=self.sqlite_dbs, **self.cacher_params,
                )
                CacherThread(**cacher_thread_params).start()
            except TypeError as e:
                msg = f"Incorrect `cacher_params` for GeneFabClient(): {e}"
                raise GeneFabConfigurationException(msg)
