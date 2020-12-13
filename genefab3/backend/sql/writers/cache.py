from genefab3.config import RESPONSE_CACHE, ZLIB_COMPRESS_RESPONSE_CACHE
from genefab3.config import RESPONSE_CACHE_SCHEMAS, RESPONSE_CACHE_MAX_SIZE
from os import path, makedirs
from urllib.request import quote
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


def drop_cached_responses(accessions, logger, response_cache=RESPONSE_CACHE, schemas=RESPONSE_CACHE_SCHEMAS):
    """Drop response_cache table"""
    if accessions:
        logger.info("Dropping cached Flask responses")
        ensure_response_lru_cache(response_cache, schemas)
        with closing(connect(response_cache)) as sql_connection:
            for accession in accessions:
                try:
                    cursor = sql_connection.cursor()
                    query = f"""
                        SELECT context_identity FROM 'accessions_used'
                        WHERE accession = '{accession}'
                    """
                    identity_entries = cursor.execute(query).fetchall()
                    for entry in identity_entries:
                        context_identity = entry[0]
                        cursor.execute(f"""
                            DELETE FROM 'accessions_used'
                            WHERE context_identity = '{context_identity}'
                        """)
                        cursor.execute(f"""
                            DELETE FROM 'response_cache'
                            WHERE context_identity = '{context_identity}'
                        """)
                except OperationalError as e:
                    sql_connection.rollback()
                    logger.warning(
                        "Could not drop cached Flask responses for %s: %s",
                        accession, repr(e),
                    )
                else:
                    logger.info(
                        "Dropped %s cached Flask responses for %s",
                        len(identity_entries), accession,
                    )
                    sql_connection.commit()


def shrink_response_cache(logger, response_cache=RESPONSE_CACHE, max_size=RESPONSE_CACHE_MAX_SIZE):
    """Drop oldest cached responses to keep file size on disk under `max_size`"""
    logger.warning("Shrinking Flask response cache not implemented yet")
