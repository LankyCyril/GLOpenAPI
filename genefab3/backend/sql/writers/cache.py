from genefab3.config import RESPONSE_CACHE, RESPONSE_CACHE_SCHEMAS
from genefab3.config import ZLIB_COMPRESS_RESPONSE_CACHE
from os import path, makedirs
from urllib.request import quote
from json import dumps
from datetime import datetime
from sqlite3 import Binary, connect, OperationalError
from contextlib import closing

if ZLIB_COMPRESS_RESPONSE_CACHE:
    from zlib import compress
else:
    compress = lambda _:_


def ensure_response_lru_cache(response_cache=RESPONSE_CACHE, schemas=RESPONSE_CACHE_SCHEMAS):
    """Ensure parent directory exists; will fail with generic Python exceptions here or downstream if not a writable dir"""
    parent_path = path.dirname(response_cache)
    if (parent_path not in {"", "."}) and (not path.exists(parent_path)):
        makedirs(parent_path)
    if not path.exists(response_cache):
        with closing(connect(response_cache)) as sql_connection:
            sql_connection.cursor().execute("PRAGMA auto_vacuum = 1")
    with closing(connect(response_cache)) as sql_connection:
        for table, schema in schemas.items():
            sql_connection.cursor().execute(
                f"CREATE TABLE IF NOT EXISTS '{table}' {schema}",
            )
        sql_connection.commit()


def cache_response(context, response, accessions, response_cache=RESPONSE_CACHE, schemas=RESPONSE_CACHE_SCHEMAS):
    """Store response object blob in response_cache table, if possible"""
    ensure_response_lru_cache(response_cache, schemas)
    api_path = quote(context.full_path)
    blob = Binary(compress(response.get_data()))
    timestamp = int(datetime.now().timestamp())
    with closing(connect(response_cache)) as sql_connection:
        try:
            cursor = sql_connection.cursor()
            cursor.execute(f"""
                DELETE FROM 'response_cache'
                WHERE context_identity = '{context.identity}'
            """)
            action = f"""
                INSERT INTO 'response_cache' (
                    context_identity, api_path, timestamp,
                    response, nbytes, mimetype
                )
                VALUES (
                    '{context.identity}', '{api_path}', {timestamp},
                    ?, {blob.nbytes}, '{response.mimetype}'
                )
            """
            cursor.execute(action, [blob])
            for accession in accessions:
                cursor.execute(f"""
                    DELETE FROM 'accessions_used'
                    WHERE accession = '{accession}' AND
                    context_identity = '{context.identity}'
                """)
                cursor.execute(f"""
                    INSERT INTO 'accessions_used' (accession, context_identity)
                    VALUES ('{accession}', '{context.identity}')
                """)
        except OperationalError:
            sql_connection.rollback()
        else:
            sql_connection.commit()


def drop_response_lru_cache(logger, response_cache=RESPONSE_CACHE, schemas=RESPONSE_CACHE_SCHEMAS):
    """Drop response_cache table"""
    logger.info("Dropping flask response LRU cache")
    ensure_response_lru_cache(response_cache, schemas)
    with closing(connect(response_cache)) as sql_connection:
        cursor = sql_connection.cursor()
        for table, schema in schemas.items():
            cursor.execute(f"DROP TABLE IF EXISTS '{table}'")
            cursor.execute(f"CREATE TABLE '{table}' {schema}")
        sql_connection.commit()
