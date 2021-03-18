from pymongo import MongoClient
from socket import create_connection, error as SocketError
from genefab3.common.exceptions import GeneFabConfigurationException
from types import SimpleNamespace


class GeneFabClient():
    """Controls caching of metadata, data, and responses"""
 
    def __init__(self, *, adapter, flask_app, mongo_params, sqlite_params, cacher_params):
        """Initialize metadata and response cachers, pass DatasetFactory and Dataset to them"""
        self.mongo_db = self._get_mongo_db_connection(mongo_params)
        self.locale = mongo_params.get("locale", "en_US")
        self.sqlite_dbs = self._get_validated_sqlite_dbs(sqlite_params)
 
    def _get_mongo_db_connection(self, mongo_params, test_timeout=10):
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
        if set(sqlite_params) != {"blobs", "tables", "cache"}:
            msg = "Incorrect spec of SQL databases"
            raise GeneFabConfigurationException(msg)
        elif len(set(sqlite_params.values())) != 3:
            msg = "SQL databases must all be distinct to avoid name conflicts"
            raise GeneFabConfigurationException(msg)
        else:
            return SimpleNamespace(**sqlite_params)
