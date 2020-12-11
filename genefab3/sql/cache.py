from os import path, makedirs
from genefab3.config import RESPONSE_CACHE
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError
from urllib.request import quote
from flask import Response


RESPONSE_CACHE_SCHEMA = "('api_path' TEXT, 'response' BLOB, 'mimetype' TEXT)"


def ensure_response_lru_cache():
    """Ensure parent directory exists; will fail with generic Python exceptions here or downstream if not a writable dir"""
    if not path.exists(path.dirname(RESPONSE_CACHE)):
        makedirs(path.dirname(RESPONSE_CACHE))


def drop_response_lru_cache(logger, table="response_cache", schema=RESPONSE_CACHE_SCHEMA):
    """Drop response_cache table"""
    logger.info("Dropping flask response LRU cache")
    ensure_response_lru_cache()
    with closing(connect(RESPONSE_CACHE)) as sql_connection:
        cursor = sql_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS '{table}'")
        cursor.execute(f"CREATE TABLE '{table}' {schema}")
        sql_connection.commit()


def cache_response(request, response, table="response_cache", schema=RESPONSE_CACHE_SCHEMA):
    """Store response object blob in response_cache table, if possible"""
    ensure_response_lru_cache()
    api_path = quote(request.full_path)
    try:
        with closing(connect(RESPONSE_CACHE)) as sql_connection:
            cursor = sql_connection.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS '{table}' {schema}")
            cursor.execute(
                f"DELETE FROM '{table}' WHERE api_path = '{api_path}'",
            )
            query = f"""
                INSERT INTO '{table}' (api_path, response, mimetype)
                VALUES ('{api_path}', ?, '{response.mimetype}')
            """
            cursor.execute(query, [Binary(response.get_data())])
            sql_connection.commit()
    except OperationalError:
        pass


def retrieve_cached_response(request, table="response_cache"):
    """Retrieve cached response object blob from response_cache table if possible; otherwise return None"""
    api_path = quote(request.full_path)
    query = f"""SELECT * FROM '{table}' WHERE api_path = '{api_path}'"""
    try:
        with closing(connect(RESPONSE_CACHE)) as sql_connection:
            cursor = sql_connection.cursor()
            row = cursor.execute(query).fetchone()
    except OperationalError:
        row = None
    if isinstance(row, tuple) and (len(row) >= 3):
        return Response(response=row[1], mimetype=row[2])
    else:
        return None
