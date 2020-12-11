from os import path, makedirs
from genefab3.config import RESPONSE_CACHE
from urllib.request import quote
from contextlib import closing
from sqlite3 import connect, Binary, OperationalError


RESPONSE_CACHE_SCHEMA = "('api_path' TEXT, 'response' BLOB, 'mimetype' TEXT)"


def ensure_response_lru_cache():
    """Ensure parent directory exists; will fail with generic Python exceptions here or downstream if not a writable dir"""
    if not path.exists(path.dirname(RESPONSE_CACHE)):
        makedirs(path.dirname(RESPONSE_CACHE))


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


def drop_response_lru_cache(logger, table="response_cache", schema=RESPONSE_CACHE_SCHEMA):
    """Drop response_cache table"""
    logger.info("Dropping flask response LRU cache")
    ensure_response_lru_cache()
    with closing(connect(RESPONSE_CACHE)) as sql_connection:
        cursor = sql_connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS '{table}'")
        cursor.execute(f"CREATE TABLE '{table}' {schema}")
        sql_connection.commit()
