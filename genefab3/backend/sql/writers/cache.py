from os import path, makedirs
from genefab3.config import RESPONSE_CACHE
from urllib.request import quote
from json import dumps
from datetime import datetime
from sqlite3 import Binary, connect, OperationalError
from contextlib import closing


RESPONSE_CACHE_SCHEMA = """(
    'api_args' TEXT, 'api_path' TEXT, 'timestamp' INTEGER,
    'response' BLOB, 'nbytes' INTEGER, 'mimetype' TEXT
)"""


def ensure_response_lru_cache(response_cache=RESPONSE_CACHE, table="response_cache", schema=RESPONSE_CACHE_SCHEMA):
    """Ensure parent directory exists; will fail with generic Python exceptions here or downstream if not a writable dir"""
    if not path.exists(path.dirname(response_cache)):
        makedirs(path.dirname(response_cache))
        with closing(connect(response_cache)) as sql_connection:
            sql_connection.cursor().execute("PRAGMA auto_vacuum = 1")
    with closing(connect(response_cache)) as sql_connection:
        sql_connection.cursor().execute(
            f"CREATE TABLE IF NOT EXISTS '{table}' {schema}",
        )
        sql_connection.commit()


def cache_response(context, response, response_cache=RESPONSE_CACHE, table="response_cache", schema=RESPONSE_CACHE_SCHEMA):
    """Store response object blob in response_cache table, if possible"""
    ensure_response_lru_cache(response_cache, table, schema)
    api_path = quote(context.full_path)
    api_args = quote(dumps(context.complete_args, sort_keys=True))
    blob = Binary(response.get_data())
    timestamp = int(datetime.now().timestamp())
    try:
        with closing(connect(response_cache)) as sql_connection:
            cursor = sql_connection.cursor()
            cursor.execute(
                f"DELETE FROM '{table}' WHERE api_args = '{api_args}'",
            )
            action = f"""
                INSERT INTO '{table}' (
                    api_args, api_path, timestamp,
                    response, nbytes, mimetype
                )
                VALUES (
                    '{api_args}', '{api_path}', {timestamp},
                    ?, {blob.nbytes}, '{response.mimetype}'
                )
            """
            cursor.execute(action, [blob])
            sql_connection.commit()
    except OperationalError:
        pass


def drop_response_lru_cache(logger, response_cache=RESPONSE_CACHE, table="response_cache", schema=RESPONSE_CACHE_SCHEMA):
    """Drop response_cache table"""
    logger.info("Dropping flask response LRU cache")
    ensure_response_lru_cache(response_cache, table, schema)
    with closing(connect(response_cache)) as sql_connection:
        cursor = sql_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS '{table}'")
        cursor.execute(f"CREATE TABLE '{table}' {schema}")
        sql_connection.commit()
