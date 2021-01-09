#!/usr/bin/env python
from flask import Flask
from genefab3.config import FLASK_APP_NAME, COMPRESSIBLE_MIMETYPES
from flask_compress import Compress
from pymongo import MongoClient
from genefab3.config import MONGO_CLIENT_PARAMETERS, MONGO_DB_NAME
from pymongo.errors import ServerSelectionTimeoutError
from genefab3.common.exceptions import GeneLabDatabaseException
from genefab3.client import GeneFabClient
from genefab3.config import LOCALE, SQLITE_BLOBS, SQLITE_TABLES, SQLITE_CACHE
from genefab3_genelab_adapter.types import GeneLabAccessionFactory, GeneLabDataset
from genefab3_genelab_adapter.config import TARGET_FILE_LOCATORS
from os import environ


app = Flask(FLASK_APP_NAME)
COMPRESS_MIMETYPES = COMPRESSIBLE_MIMETYPES
Compress(app)


mongo_client = MongoClient(**MONGO_CLIENT_PARAMETERS)
try:
    mongo_client.server_info()
except ServerSelectionTimeoutError:
    raise GeneLabDatabaseException("Could not connect (sensitive info hidden)")
else:
    mongo_db = getattr(mongo_client, MONGO_DB_NAME)


genefab3_client = GeneFabClient(
    locale=LOCALE,
    mongo_db=mongo_db,
    sqlite_blobs=SQLITE_BLOBS,
    sqlite_tables=SQLITE_TABLES,
    sqlite_cache=SQLITE_CACHE,
    AccessionFactory=GeneLabAccessionFactory,
    Dataset=GeneLabDataset,
    target_file_locators=TARGET_FILE_LOCATORS,
    cacher_start_condition=lambda: environ.get("WERKZEUG_RUN_MAIN") != "true",
)
