from pymongo import MongoClient
from socket import create_connection, error as SocketError
from genefab3.common.exceptions import GeneFabConfigurationException
from types import SimpleNamespace
from functools import partial
from genefab3.api.routes import Routes


class GeneFabClient():
    """Controls caching of metadata, data, and responses"""
 
    def __init__(self, *, adapter, mongo_params, sqlite_params, cacher_params, flask_params):
        """Initialize metadata and response cachers, pass DatasetFactory and Dataset to them"""
        self.mongo_db = self._get_mongo_db_connection(mongo_params)
        self.locale = mongo_params.get("locale", "en_US")
        self.sqlite_dbs = self._get_validated_sqlite_dbs(sqlite_params)
        self._init_routes(flask_params)
 
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
        """Route Response-generating functions to Flask endpoints"""
        if "app" in flask_params:
            route = partial(flask_params["app"].route, methods=["GET"])
        else:
            raise GeneFabConfigurationException("No Flask app specified")
        for endpoint, method in Routes().items():
            print("Routing", endpoint, "to", method)
            route(endpoint)(method)
 
    def loop(self):
        """Start background cacher thread"""
        pass
