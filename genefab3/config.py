FLASK_APP_NAME = "genefab3"
LOCALE = "en_US"

COMPRESSIBLE_MIMETYPES = [
    "text/plain", "text/html", "text/css", "text/xml",
    "application/json", "application/javascript"
]

MONGO_CLIENT_PARAMETERS = dict(serverSelectionTimeoutMS=2000)
MONGO_DB_NAME = "genefab3_testing"

SQLITE_BLOBS = "./.sqlite3/blobs.db"
SQLITE_TABLES = "./.sqlite3/tables.db"
SQLITE_CACHE = "./.sqlite3/response-cache.db"
