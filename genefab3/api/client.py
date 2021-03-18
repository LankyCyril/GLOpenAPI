from pymongo import MongoClient
from socket import create_connection, error as SocketError
from genefab3.common.exceptions import GeneFabConfigurationException
from types import SimpleNamespace
from functools import partial
from genefab3.api.routes import Routes
from genefab3.common.logger import GeneFabLogger, MongoDBLogger
from logging import StreamHandler
from genefab3.api.utils import is_debug
from genefab3.common.exceptions import traceback_printer, exception_catcher


class GeneFabClient():
    """Routes Response-generating methods, continuously caches metadata and responses"""
 
    def __init__(self, *, adapter, mongo_params, sqlite_params, cacher_params, flask_params, logger_params=None):
        """Initialize metadata cacher (with adapter), response cacher, routes"""
        self.mongo_db = self._get_mongo_db_connection(mongo_params)
        self.locale = mongo_params.get("locale", "en_US")
        self.sqlite_dbs = self._get_validated_sqlite_dbs(sqlite_params)
        self._init_routes(flask_params)
        self._init_warning_handlers(logger_params)
        self._init_error_handlers(flask_params, logger_params)
 
    def _get_mongo_db_connection(self, mongo_params, test_timeout=10):
        """Check MongoDB server is running, connect to database mongo_params["db_name"]"""
        mongo_client = MongoClient(**mongo_params.get("client_params", {}))
        try:
            host_and_port = (mongo_client.HOST, mongo_client.PORT)
            with create_connection(host_and_port, timeout=test_timeout):
                pass
        except SocketError as e:
            msg = "Could not connect to internal MongoDB instance"
            raise GeneFabConfigurationException(msg, error=type(e).__name__)
        else:
            if "db_name" in mongo_params:
                return mongo_client[mongo_params["db_name"]]
            else:
                msg = "MongoDB database name not specified"
                raise GeneFabConfigurationException(msg)
 
    def _get_validated_sqlite_dbs(self, sqlite_params):
        """Check target SQLite3 files are specified correctly, convert to namespace for dot-syntax lookup"""
        if set(sqlite_params) != {"blobs", "tables", "cache"}:
            msg = "Incorrect spec of SQL databases"
            raise GeneFabConfigurationException(msg)
        elif len(set(sqlite_params.values())) != 3:
            msg = "SQL databases must all be distinct to avoid name conflicts"
            raise GeneFabConfigurationException(msg)
        try:
            assert isinstance(sqlite_params["blobs"], str)
            assert isinstance(sqlite_params["tables"], str)
            assert isinstance(sqlite_params["cache"] or "", str)
        except AssertionError:
            msg_a = "SQL databases `blobs` and `tables` must be file paths; "
            msg_b = "SQL database `cache` must be a file path or None"
            raise GeneFabConfigurationException(msg_a + msg_b)
        else:
            return SimpleNamespace(**sqlite_params)
 
    def _init_routes(self, flask_params):
        """Route Response-generating methods to Flask endpoints"""
        if "app" in flask_params:
            route = partial(flask_params["app"].route, methods=["GET"])
        else:
            raise GeneFabConfigurationException("No Flask app specified")
        for endpoint, method in Routes().items():
            route(endpoint)(method)
 
    def _init_warning_handlers(self, logger_params):
        """Set up logger to write to MongoDB collection and/or to stderr"""
        if isinstance(logger_params.get("mongo_collection"), str):
            GeneFabLogger().addHandler(
                MongoDBLogger(self.mongo_db[logger_params["mongo_collection"]]),
            )
            if logger_params.get("stderr", True):
                GeneFabLogger().addHandler(StreamHandler())
 
    def _init_error_handlers(self, flask_params, logger_params):
        """Intercept all exceptions and deliver an HTTP error page with or without traceback depending on debug state"""
        if "app" in flask_params:
            if isinstance(logger_params.get("mongo_collection"), str):
                collection = self.mongo_db[logger_params["mongo_collection"]]
            else:
                collection = None
            app = flask_params["app"]
            method = traceback_printer if is_debug() else exception_catcher
            app.errorhandler(Exception)(partial(method, collection=collection))
        else:
            raise GeneFabConfigurationException("No Flask app specified")
 
    def loop(self):
        """Start background cacher thread"""
        pass
